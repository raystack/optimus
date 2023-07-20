# Optimus

[![verify workflow](https://github.com/raystack/optimus/actions/workflows/verify.yml/badge.svg)](verification)
[![publish latest workflow](https://github.com/raystack/optimus/actions/workflows/publish-latest.yml/badge.svg)](build)
[![Coverage Status](https://coveralls.io/repos/github/raystack/optimus/badge.svg?branch=main)](https://coveralls.io/github/raystack/optimus?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/raystack/optimus?logo=semantic-release)](Version)

Optimus is an easy-to-use, reliable, and performant workflow orchestrator for data transformation, data modeling, pipelines, and data quality management. It enables data analysts and engineers to transform their data by writing simple SQL queries and YAML configuration while Optimus handles dependency management, scheduling and all other aspects of running transformation jobs at scale.

<p align="center" style="margin-top:30px"><img src="./docs/static/img/optimus.svg" /></p>

## Key Features

Discover why users choose Optimus as their main data transformation tool.

- **Warehouse management:** Optimus allows you to create and manage your data warehouse tables and views through YAML based configuration.
- **Scheduling:** Optimus provides an easy way to schedule your SQL transformation through a YAML based configuration.
- **Automatic dependency resolution:** Optimus parses your data transformation queries and builds a dependency graphs automaticaly instead of users defining their source and taget dependencies in DAGs.
- **Dry runs:** Before SQL query is scheduled for transformation, during deployment query will be dry-run to make sure it passes basic sanity checks.
- **Powerful templating:** Optimus provides query compile time templating with variables, loop, if statements, macros, etc for allowing users to write complex tranformation logic.
- **Cross tenant dependency:** Optimus is a multi-tenant service, if there are two tenants registered, serviceA and serviceB then service B can write queries referencing serviceA as source and Optimus will handle this dependency as well.
- **Hooks:** Optimus provides hooks for post tranformation logic. e,g. You can sink BigQuery tables to Kafka.
- **Extensibility:** Optimus support Python transformation and allows for writing custom plugins.
- **Workflows:** Optimus provides industry proven workflows using git based specification management and REST/GRPC based specification management for data warehouse management.

## Usage

Optimus has two components, Optimus service that is the core orchestrator installed on server side, and a CLI binary used to interact with this service. You can install Optimus CLI using homebrew on macOS:

```shell
$ brew install raystack/tap/optimus
$ optimus --help

Optimus is an easy-to-use, reliable, and performant workflow orchestrator for
data transformation, data modeling, pipelines, and data quality management.

Usage:
  optimus [command]

Available Commands:
  backup      Backup a resource and its downstream
  completion  Generate the autocompletion script for the specified shell
  extension   Operate with extension
  help        Help about any command
  init        Interactively initialize Optimus client config
  job         Interact with schedulable Job
  migration   Command to do migration activity
  namespace   Commands that will let the user to operate on namespace
  playground  Play around with some Optimus features
  plugin      Manage plugins
  project     Commands that will let the user to operate on project
  resource    Interact with data resource
  secret      Manage secrets to be used in jobs
  scheduler   Scheduled/run job related functions
  serve       Starts optimus service
  version     Print the client version information

Flags:
  -h, --help       help for optimus
      --no-color   Disable colored output

Use "optimus [command] --help" for more information about a command.
```

## Documentation

Explore the following resources to get started with Optimus:

- [Guides](https://raystack.github.io/optimus/docs/guides/create-job/) provides guidance on using Optimus.
- [Concepts](https://raystack.github.io/optimus/docs/concepts/overview/) describes all important Optimus concepts.
- [Reference](https://raystack.github.io/optimus/docs/reference/api/) contains details about configurations, metrics and other aspects of Optimus.
- [Contribute](https://raystack.github.io/optimus/docs/contribute/contributing/) contains resources for anyone who wants to contribute to Optimus.

## Running locally

Optimus requires the following dependencies:

- Golang (version 1.16 or above)
- Git

Run the following commands to compile `optimus` from source

```shell
$ git clone git@github.com:raystack/optimus.git
$ cd optimus
$ make
```

Use the following command to run

```shell
$ ./optimus version
```

Optimus service can be started with

```shell
$ ./optimus serve
```

`serve` command has few required configurations that needs to be set for it to start. Read more about it in [getting started](https://raystack.github.io/optimus/docs/getting-started/configuration).

## Compatibility

Optimus is currently undergoing heavy development with frequent, breaking API changes. Current major version is zero (v0.x.x) to accommodate rapid development and fast iteration while getting early feedback from users (feedback on APIs are appreciated). The public API could change without a major version update before v1.0.0 release.

## Contribute

Development of Optimus happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Optimus.

Read our [contributing guide](https://raystack.github.io/optimus/docs/contribute/contributing) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Optimus.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/raystack/optimus/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## License

Optimus is [Apache 2.0](LICENSE) licensed.
