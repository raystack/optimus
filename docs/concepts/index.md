# Concepts

## Job

A Job is the fundamental unit of the data pipeline which enables a data transformation 
in the warehouse of choice. A job has all the basic details required to perform a scheduled
operation few of which are:
- Schedule interval
- Date from when a transformation should start executing
- How much data this job will consume at every transformation

Each job has a single base transformation, we call them **Task**. 
Some examples of jobs include:
1. BQ2BQTask - transformation from BigQuery to BigQuery in SQL
2. SparkSQLTask - transformation from BQ/GCS to BQ/GCS in SparkSQL
3. PySparkTask - transformation using python.

## Hook

Hooks are the operations that you might want to run before or after a Job. A hook is
only associated with a single parent although they can depend on other hooks within
the same job. There can be many or zero hooks for a Job as configured by the user. 
Some examples of hooks are:

1. Transporter(BQ/GCS to Kafka)
2. Predator(Auditing & Profiling for BQ)
3. BeastLagChecker
4. Http Hooks
5. Tableau view updates

Each hook has its own set of configs and share the same asset folder as the base job.
Hook can inherit configurations from the base transformation or from a global configuration
store. 
>The fundamental difference between a hook and a task is, task can have dependencies
over other jobs inside the repository whereas hook can only depend on other hooks within
the job.


## Job Specification

Optimus has a specification repository that holds all the details required to 
define a scheduled operation. Repository has a fixed folder structure which is 
maintained by Optimus CLI (opctl). Users can create and delete the jobs from the 
repository using either optimus CLI or a simple text editor like 
[VSCode](https://code.visualstudio.com/download). A sample command to create 
a new job is "opctl create job", calling this will ask for a 
few inputs which are required for the execution of this job and 
leave rest for the user to modify of its own eg, the SQL.

Following is a sample job specification:
```yaml
version: 1
name: example_job
owner: example@opctl.com
description: sample example job
schedule:
  start_date: "2021-02-18"
  end_date: "2021-02-25"
  interval: 0 3 * * *
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: bq2bq
  config:
    PROJECT: example
    DATASET: data
    TABLE: hello_table
    LOAD_METHOD: APPEND
    SQL_TYPE: STANDARD
    PARTITION_FILTER: 'event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"'
  window:
    size: 24h
    offset: "0"
    truncate_to: d
labels:
  orchestrator: optimus
dependencies:
- job: sample_internal_job
hooks:
- name: transporter
  type: post
  config:
    KAFKA_TOPIC: optimus_example-data-hello_table
    PRODUCER_CONFIG_BOOTSTRAP_SERVERS: '{{.transporterKafkaBroker}}'
    PROTO_SCHEMA: example.data.HelloTable
    ...
```

## Macros & Templates

Optimus allows using pre-defined macros/templates to make the pipelines more
dynamic and extensible. Macros can be used in Job/Hooks configurations or Assets.
Some of the macros are:

- `"{{.DEND}}"`: this macro is replaced with the current execution date
  (in YYYY-MM-DD format) of the task (note that this is the execution date
  of when the task was supposed to run, not when it actually runs). It would
  translate to a timestamp in runtime. eg, "2021-01-30T00:00:00Z"
- `"{{.DSTART}}"`: the value of this macro is DEND minus the task window. For
  the DAILY task window, DSTART is one day behind DEND, if the task window is
  weekly, DSTART is 7 days before DEND.
- `"{{.EXECUTION_TIME}}"`: the value of this marco is always the current timestamp.

You can use these in either `job.yml` configs or in assets. For example:

- Asset for a SQL query `query.sql`
```sql
select CONCAT("Hello, ", "{{.DEND}}") as message
```
- Configuration in `job.yml`
```yaml
version: 1
name: example_job
... omitting few configs ...
hooks:
- name: transporter
  config:
    BQ_TABLE: hello_table
    FILTER_EXPRESSION: event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'
```

## Configuration

Each job specification has a set of configs made with a key value pair. Keys are always 
specific to the execution unit and value could be of 3 types.
- User provided: These inputs are valus provided or configured by users at the time of creating
  via opctl or modifying the job using a text editor. 
- Task inherited: Hooks can inherit the configuration values from base transformation and
  avoid providing the same thing using `{{.TASK__<CONFIG_NAME>}}` macro. 
  For example:  
```yaml
task:
  name: bq2bq
  config:
    DATASET: playground
hooks:
  name: myhook
  config:
    MY_DATASET: {{.TASK__DATASET}}
```
- Repository global: Configs that will be shared across multiple jobs and should remain static
  can be configured in a global config store as part of tenant registration. These configs are
  available to only the registered repository and will remain same for all the jobs. Jobs can access
  them via `{{.GLOBAL__<CONFIG_NAME>}}`. 
  For example:
```yaml
task:
  name: bq2bq
  config:
    DATASET: {{.GLOBAL__COMMON_DATASET}}    
hooks:
  name: myhook
  config:
    KAFKA_BROKER: {{.GLOBAL__KAFKA_BROKERS}}
```
  At the moment we only support these configs to be registered via REST API exposed in optimus
  which will be discussed in a different section but in near future should be configurable via
  a configuration file inside the repository.


## Assets

There should be an asset folder along with the `job.yaml` file generated via `opctl` when
a new job is created. This is a shared folder across base transformation task
and all associated hooks. For example, if BQ2BQ task is selected, it should generate a 
template `query.sql` file for writing the BigQuery transformation SQL. Assets can use
macros and functions powered by [Go templating engine](https://golang.org/pkg/text/template/). 
Optimus also injects few helper functions provided in [sprig](http://masterminds.github.io/sprig/) 
library.
For example:
```sql
{{$start_time = now}}
select CONCAT("Hello, ", "{{$start_time}}") as message
```

## Scheduler

A scheduler is one of the core unit responsible for scheduling the jobs for execution
on a defined interval. By default, Optimus uses [Airflow](https://airflow.apache.org/) 
as the schedule but does support extending to different schedulers that follow
few guidelines.
TODO: Docs for supporting custom scheduler

## Dependency Resolver

A job can have a source, and a destination to start with. This source could be internal 
to optimus like another job or external like a S3 bucket. If the dependency is internal
to optimus, it is obvious that in an ETL pipeline, it is required for the dependency to
finish successfully first before the dependent job can start. This direct or indirect
dependency can be automatically inferred in job specifications based on task inputs.
For example, in BQ2BQ task, it parses the SQL transformation and look for tables that
are used as source using FROM, JOIN, etc keywords and mark them as the dependency for the
current job. Optimus call this automatic dependency resolution which happens automatically.
There are options to manually specify a dependency using the job name within the same
project if needed to.
Overall dependencies can be divided into three types
- Intra: Jobs depending on other jobs within same tenant repository
- Inter: Jobs depending on other jobs over other tenant repository
- Extra: Jobs depending on an external dependency outside Optimus

## Priority Resolver

Schedulers who support "Priorities" to handle the problem of "What to execute first"
can take the advantage of Optimus Priority Resolver. To understand this lets take an
example taking Airflow as the scheduler:

Let's say we provide limited slots to airflow i.e. 10, that means only 10 tasks can be 
executed at a time. Now these tasks could be a [Sensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/index.html) 
to check if the upstream [DAG](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html) 
is finished, or it could be a task that actually executes the transformation. 
The issue is when Airflow don’t know what to prioritise when if all of them are scheduled
to execute at the same time. If airflow keep on scheduling these sensors(which will 
never pass because upstream DAG never really got the chance to execute as airflow didn’t schedule 
the transformation task) it will get stuck in kind of a deadlock. All those 10 slots 
will be filled by sensors again and again and Airflow will take enormous time to schedule
a actual transformation. Its not really a deadlock but very similar and waste a lot of time
finding the correct task to execute like a needle in haystack. So recently we have 
taken a story to prioritise these tasks based on how many downstream dependencies are waiting 
for it. That is, in a dependency tree, whatever is at the root(depends on nothing) will always 
be prioritised first like Standardised layer dags and then it will move to downstream 
sensors and tasks.

This will help fully utilize the Scheduler capabilities

## Optimus Adapters

Optimus uses adapters to supports extensible tasks and hooks which needs to be 
registered on a central repository.

TODO

## Secret Management
TODO

## Replay & Backups
TODO

## Monitoring & Alerting
TODO
