# Architecture

Basic building blocks of Optimus are
- Optimus CLI
- Optimus Service
- Optimus Database
- Optimus Adapters
- Scheduler

### Overview

![Overview](https://github.com/odpf/optimus/blob/95372d614af47dc0140f6b96f819bf08eb62a189/docs/concepts/OptimusArchitecture_dark_07June2021.png?raw=true "OptimusArchitecture")

### Optimus CLI

`optimus` is a command line tool used to interact with the main optimus service and basic scaffolding job
specifications. It can be used to 
- Generate jobs based on user inputs 
- Add hooks to existing jobs
- Dump a compiled specification for the consumption of a scheduler
- Deployment of specifications to `Optimus Service`
- Create resource specifications for datastores
- Start optimus server

Optimus also has an admin flag that can be turned on using `OPTIMUS_ADMIN_ENABLED=1` env flag.
This hides few commands which are used internally during the lifecycle of tasks/hooks
execution.

### Optimus Service

Optimus cli can start a service that controls and orchestrates all that Optimus has to
offer. Optimus cli uses GRPC to communicate with the optimus service for almost all the 
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
only provides the building blocks which needs to be implemented by a task. A plugin is
divided in two parts, an adapter and a docker image. Docker image contains the actual
transformation logic that needs to be executed in the task and adapter helps optimus
to understand what all this task can do and help in doing it.

### Scheduler

Job adapters consumes job specifications which eventually needs to be scheduled and 
executed via a execution engine. This execution engine is termed here as Scheduler.
Optimus by default recommends using `Airflow` but is extensible enough to support any
other scheduler that satisfies some basic requirements, one of the most important
of all is, scheduler should be able to execute a Docker container.