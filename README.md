# Optimus
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![test workflow](https://github.com/odpf/optimus/actions/workflows/test.yml/badge.svg)](test)
[![build workflow](https://github.com/odpf/optimus/actions/workflows/build.yml/badge.svg)](build)

Optimus helps your organization to build & manage data pipelines with ease.

## Features
- BigQuery
    - Schedule SQL transformation
    - Query compile time templating (variables, loop, if statements, macros, etc)
    - Table creation
    - BigQuery View creation
    - Automatic dependency resolution: In BigQuery if a query references
      tables/views as source, jobs required to create these tables will be added
      as dependencies automatically and optimus will wait for them to finish first.
    - Cross tenant dependency: Optimus is a multi-tenant service, if there are two
      tenants registered, serviceA and serviceB then service B can write queries
      referencing serviceA as source and Optimus will handle this dependency as well
    - Dry run query: Before SQL query is scheduled for transformation, during
      deployment query will be dry-run to make sure it passes basic sanity
      checks
    - Sink BigQuery tables to Kafka [using additional hook]
- Extensibility to support Python transformation
- Git based specification management
- REST/GRPC based specification management
- Multi-tenancy
- Pluggable transformation

## Getting Started

Optimus has two components, Optimus service that is the core orchestrator installed
on server side, and a CLI binary(`opctl`) used to interact with this service.

### Compiling from source

#### Prerequisites

Optimus requires the following dependencies:
* Golang (version 1.16 or above)
* Git

#### Opctl

Run the following commands to compile `opctl` from source
```shell
git clone git@github.com:odpf/optimus.git
cd optimus
make build-ctl
cp ./opctl /usr/bin # optional - copy the executables to a location in $PATH
```
The last step isn't necessarily required. Feel free to put the compiled executable anywhere you want.

Test if `opctl` is working as expected
```shell
./opctl version
```

#### Optimus

Run the following commands to compile `optimus` from source
```shell
git clone git@github.com:odpf/optimus.git
cd optimus
make build-optimus
```
Use the following command to run
```shell
./optimus
```
Note: without required arguments, optimus won't be able to start.


**Optimus Service configuration**

Configuration inputs can either be passed as command arguments or set as environment variable

| command                | env name               | required | description                                                       |
| ---------------------- | ---------------------- | -------- | ----------------------------------------------------------------- |
| server-port            | SERVER_PORT            | N        | port on which service will listen for http calls, default. `8080` |
| log-level              | LOG_LEVEL              | N        | log level - DEBUG, INFO, WARNING, ERROR, FATAL                    |
| ingress-host           | INGRESS_HOST           | Y        | e.g. optimus.example.io:80                                        |
| db-host                | DB_HOST                | Y        |                                                                   |
| db-name                | DB_NAME                | Y        |                                                                   |
| db-user                | DB_USER                | Y        |                                                                   |
| db-password            | DB_PASSWORD            | Y        |                                                                   |
| app-key                | APP_KEY                | Y        | 32 character random hash used to encrypt secrets                  |


## Credits

This project exists thanks to all the [contributors](https://github.com/odpf/optimus/graphs/contributors).

## License
Optimus is [Apache 2.0](LICENSE) licensed.