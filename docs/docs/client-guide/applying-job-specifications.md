# Applying Job Specifications
Once you have the job specifications ready, let’s try to deploy the jobs to the server by running this command:

```shell
$ optimus job replace-all --verbose
```
Note: add --config flag if you are not in the same directory with your client configuration (optimus.yaml).

This replace-all command works per project or namespace level and will try to compare the incoming jobs and the jobs 
in the server. You will find in the logs how many jobs are new, modified, and deleted based on the current condition.

```shell
$ optimus job replace-all --verbose

> Validating namespaces
validation finished!

> Replacing all jobs for namespaces [sample_namespace]
> Receiving responses:
[sample_namespace] received 1 job specs
[sample_namespace] found 1 new, 0 modified, and 0 deleted job specs
[sample_namespace] processing job job1
[sample_namespace] successfully added 1 jobs
replace all job specifications finished!
```


You might notice based on the log that Optimus tries to find which jobs are new, modified, or deleted. This is because 
Optimus will not try to process every job in every single `replace-all` command for performance reasons. If you have 
needs to refresh all of the jobs in the project from the server, regardless it has changed or not, do run the below command:

```shell
$ optimus job refresh --verbose
```

This refresh command is not taking any specifications as a request. It will only refresh the jobs in the server.

Also, do notice that these **replace-all** and **refresh** commands are only for registering the job specifications in the server, 
including resolving the dependencies. After this, you can compile and upload the jobs to the scheduler using the 
`scheduler upload-all` [command](uploading-jobs-to-scheduler.md).

Note: Currently Optimus does not provide a way to deploy only a single job through CLI. This capability is being 
supported in the API.
