# Concepts

## Job

A Job is the fundamental unit of the data pipeline which enables a data transformation in the warehouse of choice. Some examples of jobs include:
1. BQ2BQTask - does transformation from BQ to BQ in SQL
2. SparkSQLTask - does transformation from BQ/GCS to BQ/GCS in SparkSQL
3. PySparkTask - does transformation using python.



## Hook

Hooks are the operations that you might want to run before or after the Job. Hooks are only associated with a Job and they cannot be associated with other hooks. There can be many or zero hooks for a Job as configured by the user. Some examples of hooks are:

1. Transporter(BQ/GCS to Kafka)
2. BeastLagChecker
3. Predator
4. Http Hooks
5. Tableau Integration

Hooks can be of two types: `pre` or `post` and they run with the same schedule as the job.



## Job Specification

Optimus has a specification repository that holds all the details required to define a scheduled operation. Repository has a fixed folder structure which is maintained by Optimus CLI (opctl). Users can create and delete the jobs from the repository using either optimus CLI or a simple text editor like VSCode. A sample command to create a new job is "opctl job create", calling this will ask for a few inputs from the user which are required for the execution of this job and leave rest for the user to modify of its own eg, the SQL.

Following is a sample job specification:
```yaml
version: 1
name: example_job
owner: example@opctl.com
schedule:
  start_date: "2021-02-18"
  interval: 0 3 * * *
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: bq2bq
  config:
    DATASET: data
    JOB_LABELS: owner=optimus
    LOAD_METHOD: APPEND
    PROJECT: example
    SQL_TYPE: STANDARD
    TABLE: hello_table
    TASK_TIMEZONE: UTC
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies: []
hooks:
- name: transporter
  type: post
  config:
    KAFKA_TOPIC: optimus_example-data-hello_table
    PRODUCER_CONFIG_BOOTSTRAP_SERVERS: '{{.transporterKafkaBroker}}'
    PROTO_SCHEMA: example.data.HelloTable
```


## Scheduler

A scheduler is defined within Optimus, based on which Job specifications are compiled and scheduled for run with the defined configurations. By default, Optimus uses Airflow as the scheduler, and can easily be plugged with other schedulers.


## Dependency Resolver

Dependencies between multiple data pipeline jobs are automatically taken care of with in or cross data pipelines managed by Optimus. Users can also specify explicit dependencies if needed.


## Priority Resolver

Optimus also automatically figures out the priorties for all jobs. It optimizes for full utilization of scheduler's execution slots.


## Macros & Templates

Optimus allows to use pre-defined macros/templates to make the pipelines more dynamic and extensible. Macros can be used in SQL queries or Job/Hooks configurations. Some of the macros are:

- `"{{.DEND}}"`: this macro is replaced with the current execution date (in YYYY-MM-DD format) of the task (note that this is the execution date of when the task was supposed to run, not when it actually runs). It would translate to a timestamp in runtime. eg, "2021-01-30T00:00:00Z"
- `"{{.DSTART}}"`: the value of this macro is DEND minus the task window. For the DAILY task window, DSTART is one day behind DEND, if the task window is weekly, DSTART is 7 days before DEND.
- `"{{.EXECUTION_TIME}}"`: the value of this marco is always the current timestamp.

These macros can be used in a SQL query eg,
```sql
select CONCAT("Hello, ", "{{.DEND}}") as message
```


## Secret Management
## Replay & Backups
## Alerting
