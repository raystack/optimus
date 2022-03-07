- Feature Name: Improve Time & Flow of the core Optimus Job Deployment, Replay, and Backup
- Status: Draft
- Start Date: 2022-02-16
- Authors: Arinda & Sravan

# Summary

It is observed that the deployment of a project with more than 1000 jobs took around 6 minutes to complete, and the 
replay request for the same project took around 4 minutes. An analysis to improve the time taken for this is needed, 
especially if the project will be broke down to multiple namespaces. This will cause problem, as 10 namespaces might 
can consume 6 minutes * 10 times.

# Technical Design

## Background :

Understanding the current process of the mentioned issues:
### Deploy
* Resolving dependencies for all the jobs in the requested project
* Resolving priorities for all the jobs
* Compiling the jobs within requested namespace
* Uploading compiled jobs to storage

### Replay Request
* Resolving dependencies for all the jobs in the requested project
* Clearing scheduler dag run(s)

### Backup Request
* Resolving dependencies for all the jobs in the requested project
* Duplicate table

All the processes above need dependency resolution. When resolving dependency, it is being done for ALL the jobs in the 
project, regardless of the namespace and regardless if it has changed or not. For every job (bq2bq for example), Optimus 
will call each of the jobs the GenerateDependencies function in the plugin, and do a dry run hitting the Bigquery API. 
This process has been done in parallel.

To simulate, let’s say there are 1000 bq2bq jobs in a project.

```
    Job     |   Upstream
--------------------------
    A       |   -
    B       |   A
    C       |   A, B
    D       |   C       
    ...     |   ...
```

There is a change in job C, that it no longer has dependency to job A. When it happens, when deploying, currently 
Optimus will resolve dependencies for all 1000 jobs. While in fact, the changed dependencies will only happen for job C. 
There is only a slight difference where upstream is no longer A and B but only B.

Currently, Optimus is resolving dependencies every time it is needed because it is not stored anywhere, due to keep 
changing dependencies. However, now we are seeing a timing problem, and the fact that not all jobs need to be dependency 
resolved everytime there’s a deployment, a modification can be considered.

As part of this issue, we are also revisiting the current flow of job deployment process.

## Expected Behavior

### Deploy Job
Accepts the whole state of the namespace/project. What is not available will be deleted.
* Identify the added / modified / deleted jobs
* Resolve dependency only for the added or modified jobs and persist the dependency to DB
* Do priority resolution for all jobs in the project
* Compile all jobs in the project
* Upload the compiled jobs

The difference between the expected behavior and current implementation is that we will only resolve dependency for
what are necessary, and we will compile all the jobs in the project regardless the namespace. Compile and deploy all 
jobs in the project is necessary to avoid below use case:

Let's say in a single project, lies these 4 jobs. Job C depend on Job B, job B depend on Job A, and Job A and Job D are 
independent. Notice that Job C is in a different namespace.
```
Job A (Namespace 1)             : weight 100
|-- Job B (Namespace 1)         : weight 90
|   |-- Job C (Namespace 2)     : weight 80
|-- Job D (Namespace 1)         : weight 100
```
Now let's say Job E (Namespace 1) is introduced and Job B is no longer depend directly on Job A, but instead to the new 
Job E.
```
Job A (Namespace 1)             : weight 100
|-- Job E (Namespace 1)         : weight 90
|   |-- Job B (Namespace 1)     : weight 80
|       |-- Job C (Namespace 2) : weight 70
|-- Job D (Namespace 1)         : weight 100
```
Notice that Job C priority weight has been changed. This example shows that even though the changes are in Namespace 1, 
the other namespace is also affected and needs to be recompiled and deployed.

### Create Job
Accept a single/multiple jobs to be created and deployed.
* Resolve dependency for the requested job and persist the dependency to DB
* Do priority resolution for all jobs in the project
* Compile all jobs in the project
* Upload the compiled jobs

### Delete Job
* Identify any dependent jobs using dependencies table
* Delete only if there are no dependencies
TBD: Doing soft delete or move the deleted jobs to a different table

### Refresh Job
Using current state of job that has been stored, redo the dependency resolution, recompile, redeploy.
Can be useful to do clean deploy or upgrading jobs.
* Resolve dependency for all jobs in the namespace/project and persist to DB
* Do priority resolution for all jobs in the project
* Compile all jobs in the project
* Upload the compiled jobs

### Create Resource
* Deploy the requested resource
* Identify jobs that are dependent to the resource
* Resolve dependency for the jobs found
* Compile all jobs in the project
* Upload the compiled jobs
An explanation of this behaviour can be found in `Handling Modified View` section

### Delete Resource
* Identify jobs that are dependent to the requested resource
* Delete only if there are no dependencies

### Replay & Backup
* Get the dependencies from the dependencies table.
* Continue to build the tree.


## Approach :

### Checking which jobs are modified?
Currently, Optimus receives all the jobs to be deployed, compares which one to be deleted and which one to keep, 
resolves and compiles them all. Optimus does not know the state of which changed.

One of the possibilities is by using Job hash. Fetch the jobs from DB, hash and compare with the one requested.

### Persistence
The process can be optimized only if the dependencies are stored, so no need to resolve it all every time it is needed. 
Currently, this is the struct of JobSpec in Optimus:

```go
type JobSpec struct {
    ID             uuid.UUID
    Name           string
    Dependencies   map[string]JobSpecDependency
    ....
}

type JobSpecDependency struct {
    Project *ProjectSpec
    Job     *JobSpec
    Type    JobSpecDependencyType
}
```

The Dependencies field will be filled with inferred dependency after dependency resolution is finished.

We can have a new table to persist the job ID dependency.


```
    job_id              |   UUID
    job_dependency_id   |   UUID
```

Example
```
    Job     |   Upstream
---------------------------
    A       |   -
    B       |   A
    C       |   A
    C       |   B
    D       |   C
    ...     |   ...
```

If now C has been modified to have upstream of only B, means:
* Record with jobID C will be deleted
* Insert 1 new record: C with dependency B

Advantages:
* Data is available even though there are pod restarts.
* Better visibility of current dependencies.

Disadvantages:
* Additional time to write/read from DB

### Event-Based Mechanism in Deployment
Revisiting the process of deployment:
```
Step                |   Deploy      | Create Job    | Refresh
---------------------------------------------------------------
Resolve dependency  |   Diff        | Requested     | All
Resolve priority    |   All         | All           | All
Compile             |   All         | All           | All
Upload              |   All         | All           | All
```
Notice that priority resolution, compilation, and upload compiled jobs needs to be done for all the jobs in the project 
for all the namespaces. Each of the request can be done multiple times per minute and improvisation to speed up the
process is needed.

Whenever there is a request to do deployment, job creation, and refresh, Optimus will do dependency resolution based 
on each of the cases. After it finishes, it will push an event to be picked by a worker to do priority resolution, 
compilation, and upload asynchronously. There will be deduplication in the event coming in, to avoid doing duplicated 
process.

There will be a get deployment status API introduced to poll whether these async processes has been finished or not.

### Handling Dependency Resolution Failure
Currently, whenever there is a single jobs that is failing in dependency resolution, and it is within the same 
namespace as requested, it will fail the entire process. We are avoiding the entire deployment pipeline to be blocked 
by a single job failure, but instead sending it as part of the response and proceeding the deployment until finished. 
Only the failed jobs will not be deployed. There will be metrics being added to add more visibility around this.

### Handling Modified View
A BQ2BQ job can have a source from views. For this job, the dependency will be the underlying tables of the view. Let's 
simulate a case where there is a change in the view source.

In a project, there is view `X` that querying from table `A` and `B`. There is also table `C` that querying from View 
`X`. The job dependencies for this case can be summarized as:
```
    Job     |   Upstream
---------------------------
    A       |   -
    B       |   -
    C       |   A
    C       |   B
```
Job C has dependency to job `A` and `B`, even though it is querying from view `X`.

Imagine a case where view `X` is modified, for example no longer querying from `A` and `B`, but instead only from `A`.
Job `C` dependency will never be updated, since it is not considered as modified. There should be a mechanism where if
a view is updated, it will also resolve the dependency for the jobs that depend on the view.

To make this happen, there should be a visibility of which resources are the sources of a job, for example which job is 
using this view as a destination and querying from this view. Optimus is a transformation tool, in the job spec we store 
what is the transformation destination of the job. However, we are not storing what are the sources of the transformation. 
The only thing we have is job dependency, not resource.

We can add a Source URNs field to the jobs specs, or create a Job Source table. Whenever there is a change in a view
through Optimus, datastore should be able to request the dependency resolution for the view's dependent and having the
dependencies updated. We will also provide the mechanism to refresh jobs.

### CLI Perspective
Deploy job per namespace (using DeployJobSpecification rpc)
```
optimus job deploy --namespace --project
```

Deploy job for selected jobs (using CreateJobSpecification rpc)
```
optimus job deploy --namespace --project --jobs=(job1,job2)
```

Refresh the entire namespace/project
```
optimus job refresh --namespace --project
```

Refresh the selected jobs
```
optimus job refresh --namespace --project --jobs=(job1,job2)
```

## Other Thoughts:

Cache Considerations instead of persisting to PG
* Might be faster as there is no additional time for write/read from DB
* Data will be unavailable post pod restarts. Need to redo the dependency resolution overall
* Poor visibility
