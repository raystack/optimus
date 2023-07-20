# Architecture

![Architecture Diagram](/img/docs/OptimusArchitecture.png "OptimusArchitecture")

## CLI

Optimus provides a command line interface to interact with the main optimus service and basic scaffolding job 
specifications. It can be used to:
- Start optimus server
- Create resource specifications for datastores
- Generate jobs & hooks based on user inputs
- Dump a compiled specification for the consumption of a scheduler
- Validate and inspect job specifications
- Deployment of specifications to Optimus Service

## Server

Optimus Server handles all the client requests from direct end users or from airflow over http & grpc. The functionality 
of the server can be extended with the support of various plugins to various data sources & sinks. Everything around 
job/resource management is handled by the server except scheduling of jobs.

## Database
Optimus supports postgres as  the main storage backend. It is the source of truth for all user specifications, 
configurations, secrets, assets. It is the place where all the precomputed relations between jobs are stored.

## Plugins
Currently, Optimus doesn’t hold any logic and is not responsible for handling any specific transformations. This 
capability is extended through plugins and users can customize based on their needs on what plugins to use. Plugins can 
be defined through a yaml specification. At the time of execution whatever the image that is configured in the plugin 
image will be executed.

## Scheduler (Airflow)
Scheduler is responsible for scheduling all the user defined jobs. Currently, optimus supports only Airflow as 
the scheduler, support for more schedulers can be added.
