# Verifying the Jobs
Minimize the chances of having the job failed in runtime by validating and inspecting it before deployment.

## Validate Jobs
Job validation is being done per namespace. Try it out by running this command:

```shell
$ optimus job validate --namespace sample_namespace --verbose
```


Make sure you are running the above command in the same directory as where your client configuration (optimus.yaml) 
is located. Or if not, you can provide the command by adding a config flag.

By running the above command, Optimus CLI will try to fetch all of the jobs under sample_namespace’s job path that 
has been specified in the client configuration. The verbose flag will be helpful to print out the jobs being processed. 
Any jobs that have missing mandatory configuration, contain an invalid query, or cause cyclic dependency will be pointed out.

## Inspect Job
You can try to inspect a single job, for example checking what are the upstream/dependencies, does it has any downstream, 
or whether it has any warnings. This inspect command can be done against a job that has been registered or not registered 
to your Optimus server.

To inspect a job in your local:
```shell
$ optimus job inspect <job_name>
```


To inspect a job in the server:
```shell
$ optimus job inspect <job_name> --server
```

You will find mainly 3 sections for inspection:
- **Basic Info**:
  Optimus will print the job’s specification, including the resource destination & sources (if any), and whether it has any soft warning.
- **Upstreams**:
  Will prints what are the jobs that this job depends on. Do notice there might be internal upstreams, external (cross-server) upstreams, HTTP upstreams, and unknown upstreams (not registered in Optimus).
- **Downstreams**:
  Will prints what are the jobs that depends on this job.
