# Concepts

## Project

A Project/Tenant represents a group of Jobs, Resources, Scheduler with the specified 
configurations and infrastructure. A Project contains multiple user created Namespaces, 
and each Namespace contains multiple Jobs/Hooks and configurations.

## Namespace

A Namespace represents a grouping of specified Jobs and Resources which can be accessible
only through the namespace owners. User may override the Project configuration or define
configuration locally at the namespace level. A namespace always belongs to a Project. 
All Namespaces of a Project share same infrastructure and the Scheduler. They share all
the accesses and secrets provided by the Project, however, they cannot access or modify 
the Jobs and Datastore Resources of other namespaces.

A use case for Namespace could be when multiple teams want to re-use the existing 
infrastructure but want to maintain their specifications like Jobs, Resources etc 
independently. The namespace's name can be chosen by user or can be provided by the
authentication service.


## Optimus cli

Optimus provides a cli used to start Optimus service using `serve` command and a
lot of other features like interacting with the remote/local optimus service, bootstrapping
specifications, validating, testing, etc. Although it is not necessary to use cli
and GRPC/REST can also be used directly which is what CLI does internally for communication
with the service. 

## Job

A Job is the fundamental unit of the data pipeline which enables a data transformation 
in the warehouse of choice. A job has all the basic details required to perform a scheduled
operation few of which are:
- Schedule interval
- Date from when a transformation should start executing
- How much data this job will consume at every transformation

Each job has a single base transformation, we call them **Transformer** or **Task**. 
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
maintained by Optimus CLI. Users can create and delete the jobs from the 
repository using either optimus CLI or a simple text editor like 
[VSCode](https://code.visualstudio.com/download). A sample command to create 
a new job is "optimus create job", calling this will ask for a 
few inputs which are required for the execution of this job and 
leave rest for the user to modify of its own eg, the SQL.

Following is a sample job specification:
```yaml
# specification version, for now just keep it fixed unless optimus has any
# breaking change
version: 1

# unique name for the job, try to use simple ascii characters and less than 200 chars
# to keep scheduler db's happy
name: example_job

# owner of the job
owner: example@example.com

# description of this job, what this do
description: sample example job

# configure when it should start, when the job should stop executing and what
# interval scheduler should use for execution
schedule:
  # time format should be RFC3339
  start_date: "2021-02-18"
  end_date: "2021-02-25"
  
  # supports standard cron notations
  interval: 0 3 * * *

# extra modifiers to change the behavior of the job
behavior:
  
  # should the job wait for previous runs to finish successfully before executing
  # next run, this will make it execute in sequence
  depends_on_past: false
  
  # if start_date is set in the past, and catchup is true, it will allow scheduler
  # to automatically backfill history executions till it reaches today
  catch_up: true
  
  # retry behaviour of this job if it fails to successfully complete in first try
  retry:
    
    # maximum number of tries before giving up
    count: 3
    
    # delay between retries
    delay: "15m"
    
    # allow progressive longer waits between retries by using exponential backoff algorithm 
    # on retry delay (delay will be converted into seconds)
    exponential_backoff: false
    
# transformation task configuration for this job
task:
  # name of the task type
  name: bq2bq
  
  # configuration passed to the task before execution
  config:
    PROJECT: example
    DATASET: data
    TABLE: hello_table
    LOAD_METHOD: APPEND
    SQL_TYPE: STANDARD
    PARTITION_FILTER: 'event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"'
  
  # time window, could be used by task for running incremental runs instead of processing
  # complete past data at every iteration
  window:
    
    # size of incremental window
    # eg: 1h, 6h, 48h, 2h30m
    size: 24h
    
    # shifting window forward of backward in time, by default it is yesterday
    offset: "0"
    
    # truncate time window to nearest hour/day/week/month
    # possible values: h/d/w/M
    truncate_to: d
    
# labels gets passed to task/hooks
# these can be used to attach metadata to running transformation
# discovering usage, identifying cost, grouping identities, etc
labels:
  orchestrator: optimus
  
# static dependencies that can be used to wait for upstream job to finish
# before this job can be started for execution 
dependencies:

  # list `job: <jobname>`
  - job: sample_internal_job
  
# adhoc operations marked for execution at different hook points
# accepts a list
hooks:
  - # name of the hook
    name: transporter
    
    # where & when to attach this process
    type: post
    
    # configuration passed to hook before execution
    config:
      KAFKA_TOPIC: optimus_example-data-hello_table
      
      # configuration being inherited from project level variables
      PRODUCER_CONFIG_BOOTSTRAP_SERVERS: '{{.GLOBAL__TransporterKafkaBroker}}'
      
      PROTO_SCHEMA: example.data.HelloTable
```

## Macros & Templates

Optimus allows using pre-defined macros/templates to make the pipelines more
dynamic and extensible. Macros can be used in Job/Hooks configurations or Assets.
Some macros are:

- `"{{.DEND}}"`: this macro is replaced with the current execution date
  (in yyyy-mm-ddThh:mm:ssZ format) of the task (note that this is the execution date
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
Macros can be chained together via pipe-sign with predefined functions.
- `Date`: Converters Timestamp to Date. For example
```sql
SELECT * FROM table1
WHERE DATE(event_timestamp) < '{{ .DSTART|Date }}'
```

## Configuration

Each job specification has a set of configs made with a key value pair. Keys are always 
specific to the execution unit and value could be of 3 types.
- User provided: These inputs are values provided by users at the time of creating
  via optimus cli or modifying the job using a text editor. 
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

There could be an asset folder along with the `job.yaml` file generated via `optimus` when
a new job is created. This is a shared folder across base transformation task
and all associated hooks. For example, if BQ2BQ task is selected, it should generate a 
template `query.sql` file for writing the BigQuery transformation SQL. Assets can use
macros and functions powered by [Go templating engine](https://golang.org/pkg/text/template/). 
Optimus also injects few helper functions provided in [sprig](http://masterminds.github.io/sprig/) 
library.
For example:
```sql
{{ $name := "admin" }}
select CONCAT("Hello, ", "{{.name}}") as message
```

Section of code can be imported from different asset files using 
[template](https://golang.org/pkg/text/template/#hdr-Actions). For example:

- File `partials.gtpl`
```sql
DECLARE t1 TIMESTAMP;
```
- Another file `query.sql`
```sql
{{template "partials.gtpl"}}
SET t1 = '2021-02-10T10:00:00+00:00';
```
During execution `query.sql` will be rendered as:
```sql
DECLARE t1 TIMESTAMP;
SET t1 = '2021-02-10T10:00:00+00:00';
```
whereas `partials.gtpl` will be left as it is because file was saved with `.gtpl`
extension.

Similarly, a single file can contain multiple blocks of code that can function
as macro of code replacement. For example:
- `file.data` 
```
  Name: {{ template "name"}}, Gender: {{ template "gender" }}
```
- `partials.gtpl`
```
  {{- define "name" -}} Adam {{- end}}
  {{- define "gender" -}} Male {{- end}}
```
This will render `file.data` as
```
Name: Adam, Gender: Male
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
- Extra: Jobs depending on an external dependency outside Optimus [TODO]

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

This will help fully utilize the Scheduler capabilities.

## Optimus Plugins

Optimus's responsibilities are currently divided in two parts, scheduling a transformation [task](#Job) and running one time action to create or modify a [datastore](#Datastore) resource. Defining how a datastore is managed can be easy and doesn't leave many options for configuration or ambiguity although the way datastores are implemented gives developers flexibility to contribute additional type of datastore, but it is not something we do every day.

Whereas tasks used in jobs that define how the transformation will execute, what configuration does it need as input from user, how does this task resolves dependencies between each other, what kind of assets it might need. These questions are very open and answers to them could be different in  different organisation and users. To allow flexibility of answering these questions by developers themselves, we have chosen to make it easy to  contribute a new kind of task or even a hook. This modularity in Optimus is achieved using plugins.

> Plugins are self-contained binaries which implements predefined protobuf interfaces to extend Optimus functionalities.

Optimus can be divided in two logical parts when we are thinking of a pluggable model, one is the **core** where everything happens which is common for all job/datastore, and the other part which could be variable and needs user specific definitions of how things should work which is a **plugin**.

## Datastore

Optimus datastores are managed warehouses that provides CRUD on resources attached to it. Each warehouse supports fixed set of resource types, each type has its own specification schema.

At the moment, Optimus supports BigQuery datastore for 3 types of resources:
- [Dataset](https://cloud.google.com/bigquery/docs/datasets-intro)
- [Table](https://cloud.google.com/bigquery/docs/tables-intro)
- [Standard View](https://cloud.google.com/bigquery/docs/views-intro)



## Secret Management
TODO

## Replay & Backups
TODO

## Monitoring & Alerting
TODO
