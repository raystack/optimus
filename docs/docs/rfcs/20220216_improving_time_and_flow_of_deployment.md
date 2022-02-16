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

### Affected Areas
Resolving dependencies are being done when doing deployment, create jobs, replay, backup, and job deletion (to check if a to-be-deleted job still has downstream).

#### Deployment
* Identify the modified Jobs 
* Delete the jobs from repository which are no longer there in the request
* Identify the dependencies only for modified ones & persist the dependencies
* Load all dependencies for all jobs in the entire project
* Continue on the priority resolution and other processes

#### Create Job
* Identify the dependencies only for modified ones & persist the dependencies
* Load all dependencies for all jobs in the entire project
* Continue on the priority resolution and other processes

#### Replay & Backup
* Instead of doing the dependency resolution, replay and backup will get the dependencies from the job_dependency table.
* Continue to build the tree.

#### Job Deletion
* Instead of doing the dependency resolution, before deleting a job, there will be a check to job_dependency table whether there is a record with this job ID as an upstream (dependency).


### Other Thoughts:

Cache Considerations instead of persisting to PG
* Might be faster as there is no additional time for write/read from DB
* Data will be unavailable post pod restarts. Need to redo the dependency resolution overall
* Poor visibility
