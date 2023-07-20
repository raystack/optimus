# Job

A Job is the fundamental unit of the data pipeline which enables a data transformation in the warehouse of choice.
A user can configure various details mentioned below for the job:

- Schedule interval
- Date from when a transformation should start executing
- Task & Hooks
- Assets needed for transformation
- Alerts

Job specifications are being compiled to later be processed by the scheduler. Optimus is using Airflow as the scheduler,
thus it is compiling the job specification to DAG (_Directed Acryclic Graph_) file.

Each of the DAG represents a single job, which consists of:

- Airflow task(s). Transformation tasks and hooks will be compiled to Airflow tasks.
- Sensors, only if the job has dependency.

Each job has a single base transformation, we call them **Task** and might have the task pre or/and post operations,
which are called **Hooks**.

## Task

A task is a main transformation process that will fetch data, transform as configured, and sink to the destination.
Each task has its own set of configs and can inherit configurations from a global configuration store.

Some examples of task are:

- BQ to BQ task
- BQ to Email task
- Python task
- Tableau task
- Etc.

## Hook

Hooks are the operations that you might want to run before or after a task. A hook is only associated with a single
parent although they can depend on other hooks within the same job. There can be one or many or zero hooks for a Job as
configured by the user. Some examples of hooks are:

- [Predator](https://github.com/raystack/predator) (Profiling & Auditing for BQ)
- Publishing transformed data to Kafka
- Http Hooks

Each hook has its own set of configs and shares the same asset folder as the base job. Hook can inherit configurations
from the base transformation or from a global configuration store.

The fundamental difference between a hook and a task is, a task can have dependencies over other jobs inside the
repository whereas a hook can only depend on other hooks within the job.

## Asset

There could be an asset folder along with the job.yaml file generated via optimus when a new job is created. This is a
shared folder across base transformation task and all associated hooks. Assets can use macros and functions powered by
[Go templating engine](https://golang.org/pkg/text/template/).

Section of code can be imported from different asset files using template. For example:

- File partials.gtpl

```gotemplate
DECLARE t1 TIMESTAMP;
```

- Another file query.sql

```gotemplate
{{template "partials.gtpl"}}
SET t1 = '2021-02-10T10:00:00+00:00';
```

During execution query.sql will be rendered as:

```gotemplate
DECLARE t1 TIMESTAMP;
SET t1 = '2021-02-10T10:00:00+00:00';
```

whereas **partials.gtpl** will be left as it is because file was saved with .gtpl extension.

Similarly, a single file can contain multiple blocks of code that can function as macro of code replacement. For example:

- file.data

```gotemplate
Name: {{ template "name"}}, Gender: {{ template "gender" }}
```

- partials.gtpl

```gotemplate
{{- define "name" -}} Adam {{- end}}
{{- define "gender" -}} Male {{- end}}
```

This will render file.data as

```
Name: Adam, Gender: Male
```
