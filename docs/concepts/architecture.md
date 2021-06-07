# Architecture

Basic building blocks of Optimus are
- Opctl
- Optimus Service
- Optimus Database
- Optimus Adapters
- Scheduler

### Overview

![Overview](./OptimusArchitecture_dark_20May2021.png?raw=true "OptimusArchitecture")

### Opctl

`Opctl` is a command line tool used to interact with the main optimus service and basic scaffolding job
specifications. It can be used to 
- Generate job based on user inputs 
- Add hooks to existing jobs
- Dump a compiled specification for the consumption of a scheduler
- Deployment of specifications to `Optimus Service`

Opctl also has a Admin flag that can be turned on using `OPTIMUS_ADMIN=1` env flag.
This hides few commands which are used internally during the lifecycle of tasks/hooks
execution.

### Optimus Service

Optimus Service is the mastermind that controls and orchestrates the dance of all
job. Opctl uses GRPC to communicate with the optimus service for almost all the 
operations that takes `host` as the flag. Service also exposes few REST endpoints
that can be used with simple curl request for registering a new project or checking
the status of a job, etc.

As soon as jobs are ready in a repository, a deployment request is sent to the service
with all the specs(normally in yaml) which are parsed and stored in the database.
Once these specs are stored, each of them are compiled to generate a scheduler parsable
job format which will be eventually consumed by a supported scheduler to execute the
job. These compiled specifications are uploaded to an **object store** which gets synced
to the scheduler.

### Optimus Database

Specifications once requested for deployment needs to be stored somewhere as a source
of truth. Optimus uses postgres as a storage engine to store raw specifications, job
assets, run details, project configurations, etc.

### Optimus Plugins

Optimus itself doesn't govern how a job is supposed to execute the transformation. It
only provides the building blocks which needs to be implemented by a task. This task
needs to be registered to a centralised adapter repository which outputs a optimus
consumable docker image. These docker images can be registered in Optimus as **Task**
or **Hooks**.

### Scheduler

Job adapters consumes job specifications which eventually needs to be scheduled and 
executed via a execution engine. This execution engine is termed here as Scheduler.
Optimus by default recommends using Airflow but is extensible enough to support any
other scheduler that satisfies some basic requirements, one of the most important
of all is, scheduler should be able to execute a Docker container.