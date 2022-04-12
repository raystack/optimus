---
id: refresh-jobs
title: Refresh Jobs
---

Jobs might need to be refreshed on certain cases, for example:
* When job source is a `view` and the source view has been modified.
* When there is some issues on job dependencies or priorities and need to do a clean up
* When jobs need to have its plugin refreshed to the latest version

Optimus refresh will try to resolve the dependencies for the **requested** jobs and deploy **all** jobs in the project. 

## Refresh jobs

Refresh all jobs in the requested project:
```shell
$ optimus job refresh --project sample-project --verbose
```
Note: use verbose flag to show list of jobs being refreshed and deployed

### Refresh jobs on selected namespaces
Use `namespaces` flag to refresh only the selected namespace. Only all the jobs in the selected namespaces will be 
refreshed, but all the jobs in the project will be deployed.
```shell
$ optimus job refresh --project sample-project --namespaces namespace-a,namespace-b
```

### Refresh only selected jobs
Use `jobs` flag to only refresh selected jobs. Only the selected jobs will be refreshed, but all the jobs in the project 
will be deployed.
```shell
$ optimus job refresh --project sample-project --namespaces namespace-a --jobs job-a,job-b
```
Note: All the selected jobs should be inside the same namespace. 