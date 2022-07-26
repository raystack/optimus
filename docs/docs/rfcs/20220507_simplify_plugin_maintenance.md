- Feature Name: Simplify Plugins
- Status: Draft
- Start Date: 2022-05-07
- Author: Saikumar

# Summary

The scope of this rfc is to simplify the release and deployment operations w.r.t the optimus plugin ecosystem.

The proposal here is to :
1. **Avoid Wrapping Executor Images** :   
Decouple the executor_boot_process and the executor as separate containers where the airflow worker launches a pod with init-container (for boot process) adjacent to executor container.
2. **Simplfy Plugin Installation** :
    - `Server end` : Install plugins declaratively at runtime instead of manually baking them into the optimus server image (in kubernetes setup).
    - `Client end` : Plugin interface for client-end is limited to support  Version, Survey Questions and Answers etc. that can be extracted out from the current plugin interface and maintained as yaml file which simplifies platform dependent plugin distribution for cli.
# Technical Design
## Background :
### Changes that trigger a new release in Optimus setup:
* Executor Image changes
* Executor Image Wrapper changes
* Plugin binary changes
* Optimus binary changes

### Release dependencies as per current design
* `Executor Image release` -> `Executor Wrapper Image release` -> `Plugin binary release` -> `Server release`
* `Plugin binary release` -> `Server release`

### 1. <u>Avoid Wrapping Executor Images</u>  : 
* The `executor_boot_process` and `executor` are coupled:
```
-- Plugin repo structure
/task/
/task/Dockerfile           -- task_image
/task/executor/Dockerfile  -- executor_image
```

Executor Wrapper Image (Task Image) :
- It's a wrapper around the executor_image to facilitate boot mechanism for executor.
- The optimus binary is downloaded during buildtime of this image.
- During runtime, it does as follow :
    - Fetch assets, secrets, env from optimus server.
    - Load the env and launches the executor process.

```
task_image 
    | executor_image
    | optimus-bin
    | entrypoint.sh (load assets, env and launch executor)
```

The `optimus-bin` and `entrypoint.sh` are baked into the `task_image` and is being maintained by task/plugin developers.

### 2. <u>Simplify Plugin Installation</u> : 
* Plugin binaries are manually installed (baked into optimus image in kubernetes setup). 
* Any change in plugin code demands re-creation of optimus image with new plugin binary, inturn demanding redeployment of optimus server. (in kubernetes setup)
* At client side, plugin binaries require support for different platforms.

## Approach :
### 1. <u>Avoid Wrapping Executor Images </u> :
* Decouple the lifecycle of the executor and the boot process as seperate containers/images.

<!-- ![Architecture](images/simplify_plugins.png) -->
<img src="images/simplify_plugins_executor.png" alt="Simplify Plugins Executor" width="800" />

**Task Boot Sequence**:
1. Airflow worker fetches env and secrets for the job and adds them to the executor pod as environment variables.
2. KubernetesPodOperator spawns init-container and executor-container, mounted with shared volume (type emptyDir) for assets.
3. `init-container` fetches assets, config, env files and writes onto the shared volume.
4. the default entrypoint in the executor-image starts the actual job.


### 2. <u>Simplify Plugin Installations</u> :
<!-- <img src="images/plugin_manager.png" alt="Plugins Manager" width="800" /> -->

#### A) Plugin Manager:
+ Currently the plugins are maintained as monorepo and versioned together. For any change in a single plugin, a new tar file containing all plugin binaries is created and released.
+ A plugin manager is required to support declarative installation of plugins so that plugins can be independently versioned, packaged and installed.
+ This plugin manager consumes a config (plugins_config) and downloads artifacts from a plugin repository.
* Optimus support for plugin manager as below.
    *  `optimus plugin install -c config.yaml` -- at server
* Support for different kinds of plugin repositories (like s3, gcs, url, local file system etc..) gives the added flexibility and options to distribute and install the plugin binaries in different ways.
* Plugins are installed at container runtime and this decouples the building of optimus docker image from plugins installations.
* Example for the plugin_config: 
```yaml
plugin:
  dir: .plugins
  artifacts:
    # local filesystem for dev
    - ../transformers/dist/bq2bq_darwin_arm64/optimus-bq2bq_darwin_arm64
    # any http uri
    - https://github.com/odpf/optimus/releases/download/v0.2.5/optimus_0.2.5_linux_arm64.tar.gz
      
 ```
#### B) Yaml Plugin Interface: (for client side simplification)
+ Currently plugins are implemented and distributed as binaries and as clients needs to install them, it demands support for different host  architectures.
+ Since CLI (client side) plugins just require details about plugin such as Version, Suevery Questions etc. the proposal here is to maintain CLI plugins as yaml files.
+ Implementation wise, the proposal here is to split the current plugin interface (which only supports interaction with binary plugins) to also accommodate yaml based plugins.
+ The above mentioned pluign manager, at server end, would be agnostic about the contents of plugin artifacts from the repository.
+ At client side, the CLI could sync the yaml files from the server to stay up-to-date with the server w.r.t plugins.
+ At this point, we have the scope to move away from binary plugins except for bq2bq plugin due to its depdendency on `ComplileAsset` and `ResolveDependency` functionalities which are required at server end (not at cli).
+ Handling Bq2Bq plugin:
    +  Move `CompileAsset` functionality as a part of Bq2bq executor.
    +  Move  `ResolveDependency` functionality to optimus core which should support dependecy-resolution on standard-sql
+ Meanwhile the Bq2bq plugin is handled, the plugin interface can be maintanined in such a way that it also supports binary plugin in addition to yaml (as backward compaitbility feature).
+ The plugin discovery logic should be to load binary if present, else load yaml file; for a single plugin.
+ Now that we have yaml files at server, CLI can sync only the yaml files from the server.
  *  `optimus plugin sync -c optimus.yaml`

* Example representation of the yaml plugin : 
```yaml
name: bq2bq
description: BigQuery to BigQuery transformation task
plugintype: task
pluginmods:
  - cli
  - dependencyresolver
pluginversion: 0.1.0-SNAPSHOT-27cb56f
apiversion: []
image: docker.io/odpf/optimus-task-bq2bq-executor:0.1.0-SNAPSHOT-27cb56f
secretpath: /tmp/auth.json
dependson: []
hooktype: ""

questions:
  - name: PROJECT
    prompt: Project ID
    help: Destination bigquery project ID
    regexp: ^[a-zA-Z0-9_\-]+$
    validationerror: invalid name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)
    minlength: 3
  - name: Dataset
    prompt: Dataset Name
    help: Destination bigquery dataset ID
    regexp: ^[a-zA-Z0-9_\-]+$
    validationerror: invalid name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)
    minlength: 3
  - name: TABLE
    prompt: Table ID
    help: Destination bigquery table ID
    regexp: ^[a-zA-Z0-9_-]+$
    validationerror: invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)
    minlength: 3
    maxlength: 1024
  - name: LOAD_METHOD
    prompt: Load method to use on destination
    help: |
      APPEND        - Append to existing table
      REPLACE       - Deletes existing partition and insert result of select query
      MERGE         - DML statements, BQ scripts
      REPLACE_MERGE - [Experimental] Advanced replace using merge query
    default: APPEND
    multiselect:
      - APPEND
      - REPLACE
      - MERGE
      - REPLACE_MERGE
      - REPLACE_ALL
defaultassets:
  - name: query.sql
    value: |
      -- SQL query goes here

      Select * from "project.dataset.table";
      
 ```
## Result:
<img src="images/simplify_plugins.png" alt="Simplify Plugins" width="800" />

* Executor boot process is standardised and extracted away from plugin developers. Now any arbitrary image can be used for executors.
* At server side, for changes in plugin (dur to plugin release), update the plugin_manager_config and restart the optimus server pod. The plugin manager is expected to reinstall the plugins.
* Client side dependency on plugins is simplified with yaml based plugins.


