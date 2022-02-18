- Feature Name: Improve Time & Flow of the core Optimus Job Deployment, Replay, and Backup
- Status: Draft
- Start Date: 2022-02-16
- Authors: Arinda & Sravan

# Summary

It is observed that the deployment of a project with more than 1000 jobs took around 6 minutes to complete, and the replay request for the same project took around 4 minutes. An analysis to improve the time taken for this is needed, especially if the project will be broke down to multiple namespaces. This will cause problem, as 10 namespaces might can consume 6 minutes * 10 times.

# Technical Design

### Background :

Understanding the current process of the mentioned issues:
#### Deploy
* Resolving dependencies
* Resolving priorities 
* Compiling
* Uploading to storage

#### Replay Request
* Resolving dependencies
* Clearing scheduler dag run

#### Backup Request
* Resolving dependencies
* Duplicate table

All the processes above need dependency resolution. When resolving dependency, it is being done for ALL the jobs in the project, regardless of the namespace and regardless if it has changed or not. For every job (bq2bq for example), Optimus will call each of the jobs the GenerateDependencies function in the plugin, and do a dry run hitting the Bigquery API. This process has been done in parallel.

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

There is a change in job C, that it no longer has dependency to job A. When it happens, when deploying, currently Optimus will resolve dependencies for all 1000 jobs. While in fact, the changed dependencies will only happen for job C. There is only a slight difference where upstream is no longer A and B but only B.

Currently, Optimus is resolving dependencies every time it is needed because it is not stored anywhere, due to keep changing dependencies. However, now we are seeing a timing problem, and the fact that not all jobs need to be dependency resolved everytime there’s a deployment, a modification can be considered.


### Approach :

#### Checking which jobs are modified?
Currently, Optimus receives all the jobs to be deployed, compares which one to be deleted and which one to keep, resolves and compiles them all. Optimus does not know the state of which changed.

One of the possibilities is by using Job hash. Fetch the jobs from DB, hash and compare with the one requested.

#### Persistence
The process can be optimized only if the dependencies are stored, so no need to resolve it all every time it is needed. Currently, this is the struct of JobSpec in Optimus:

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

Might need DB Locking to prevent accessing dependency while in the process of updating.

Advantages:
* Data is available even though there are pod restarts.
* Better visibility of current dependencies.

Disadvantages:
* Additional time to write/read from DB

### Handling Modified View
A BQ2BQ job can have a source from views. For this job, the dependency will be the underlying tables of the view. Let's simulate a case where there is a change in the view source.

In a project, there is view `X` that querying from table `A` and `B`. There is also table `C` that querying from View `X`. The job dependencies for this case can be summarized as:
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
dependencies updated. Same with job deployment, resource deployment should also be done only for the modified ones.

We can also provide a mechanism to refresh dependencies of all jobs that can be used by admins for fixing any errors, 
or to refresh when there are views updated outside Optimus.

This can be put as a flag in deploy command.
```
optimus deploy --refresh
```
or
```
optimus deploy --clean
```

### Affected Areas
Resolving dependencies are being done when doing deployment, create jobs, replay, backup, and job deletion (to check if a to-be-deleted job still has downstream).

#### Job Deployment
* Identify the modified Jobs 
* Delete the jobs from repository which are no longer there in the request
* Identify the dependencies only for modified ones & persist the dependencies
* Load all dependencies for all jobs in the entire project
* Continue on the priority resolution and other processes
Note: We will also provide --refresh/clean flag to not look for the modified jobs, but instead resolve dependencies for 
all jobs

#### Create Job
* Identify the dependencies only for modified ones & persist the dependencies
* Load all dependencies for all jobs in the entire project
* Continue on the priority resolution and other processes

#### Replay & Backup
* Instead of doing the dependency resolution, replay and backup will get the dependencies from the job_dependency table.
* Continue to build the tree.

#### Job Deletion
Instead of doing the dependency resolution, before deleting a job, there will be a check to job_dependency table whether there is a record with this job ID as an upstream (dependency).

#### Resource Deployment
* Identify the modified Resources
* External datastore will decide if the modified resource requires dependency resolution (example for BQ view)
* Job service will run the dependency resolution if needed & persist the dependencies
* Load all dependencies for all jobs in the entire project
* Continue on the priority resolution and other processes to deploy the affected jobs

#### Resource Deletion
At this time, resource deletion is not supported. However, we can enable this and validate whether a job is available 
for this resource or not.

### Other Thoughts:

Cache Considerations instead of persisting to PG
* Might be faster as there is no additional time for write/read from DB
* Data will be unavailable post pod restarts. Need to redo the dependency resolution overall
* Poor visibility
