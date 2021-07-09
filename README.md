# Optimus
[![Coverage Status](https://coveralls.io/repos/github/odpf/optimus/badge.svg?branch=master)](https://coveralls.io/github/odpf/optimus?branch=master)
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
on server side, and a CLI binary used to interact with this service.

### Compiling from source

#### Prerequisites

Optimus requires the following dependencies:
* Golang (version 1.16 or above)
* Git

Run the following commands to compile `optimus` from source
```shell
git clone git@github.com:odpf/optimus.git
cd optimus
make build
```
Use the following command to run
```shell
./optimus version
```

Optimus service can be started with
```shell
./optimus serve
```

`serve` command has few required configurations that needs to be set for it to start. Configuration can either be stored
in `.optimus.yaml` file or set as environment variable. Read more about it in [getting started](https://odpf.github.io/optimus/getting-started/configuration/).

### Installing via brew

You can install Optimus using homebrew on macOS:

```shell
brew install odpf/taps/optimus
optimus version
```

## Credits

This project exists thanks to all the [contributors](https://github.com/odpf/optimus/graphs/contributors).

## License
Optimus is [Apache 2.0](LICENSE) licensed.