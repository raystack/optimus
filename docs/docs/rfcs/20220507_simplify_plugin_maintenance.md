- Feature Name: Simplify Plugin Maintenance
- Status: Draft
- Start Date: 2022-05-07
- Authors: Saikumar

# Summary

All the plugin/task developers potentially have to build new docker images (make a release) whenever there is change in `optimus-bin` or `entrypoint.sh` (of the task_image).

Unnecessarily the plugin/task developers are burdened with this extra complexity.

The proposal here is to decouple the `boot_process` and `executor` as separate containers where the `airflow worker` launches a pod with `init-container` (for boot process) adjacent to `executor container`.

# Technical Design

## Background :
```
-- Plugin repo structure

/task/
/task/Dockerfile           -- task_image
/task/executor/Dockerfile  -- executor_image
```

**Task Image** :
- It's a wrapper around the executor image to facilitate boot mechanism for executor.
- The optimus binary is downloaded during buildtime of this image.
- During runtime, it does as follow :
    - Fetch assets, secrets, env from optimus server.
    - Load the env and launches the executor process.

**Executor Image** :
- Contains business logic implementation independent of optimus server interactions.

```
task_image 
    | executor_image
    | optimus-bin
    | entrypoint.sh (load assets, env and launch executor)
```

The `optimus-bin` and `entrypoint.sh` are baked into the `task_image` and is being maintained by task/plugin developers.

Any changes in the above, demands the plugin/task devs to make a new release, even when the core executor logic remain unchanged.


---

## Expected Outcome:
* The plugin/task developers only maintain and release `executor_image`.
* A centralized image for the boot_process is maintained by optimus core devs which will be used as init-container for the executor.


## Approach :

* Decouple the lifecycle of the executor and the boot process as seperate containers/images.

![Architecture](images/simplify_plugins.png)
<!-- <img src="images/simplify_plugins.png" alt="Architecture" width="800" /> -->

**Task Boot Sequence**:
1. KubernetesPodOperator spawns init-container and executor-container mounted with shared volume (type emptyDir).
2. `init-container` fetches assets, config, env files and writes to the shared volume.
3. `postStart` lifecycle hook in the `executor-container` loads env from files on the shared volume.
4. the default entrypoint in the executor-image starts the actual job.

```yaml
# sample task definition
apiVersion: v1
kind: Pod
metadata:
  name: {{.task}}
spec:
  # init container
  initContainers:
  - name: init-executor
    image: {{.default-init-docker-image}}
    volumeMounts:
    - mountPath: /usr/share/asserts
      name: assets-dir
  containers:
    # executor container
    - image: {{.executor-image-repo-link}}
      name : {{.executor-name}}
      volumeMounts:
        - name: assets-dir
          mountPath: /var/assets
      # entrypoint.sh
      lifecycle:
        postStart:
          exec:
            command:
                - "sh"
                - "-c"
                - >
                source ~/.env
                # more...

  # shared volume
  volumes:
    - name: assets-dir
      emptyDir: {}
```
## Other Considerations:
* An assumption here is that the `init process` to remain same for all task executors. (standardised)
* There might be scenario where the task executor might need some `customised init process`.
* One possible way to deal with this is to let plugins devs also provide `custom-init-image`
along with `executor-image` which will fallback to a `default-init-image` if not provided.
* Supporting the `custom-init-image` would require changes in plugin interfaces and rendering airflow dag.
