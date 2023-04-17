# Organizing Specifications
Optimus supports two ways to deploy specifications
- REST/GRPC
- Optimus CLI deploy command

When using Optimus CLI to deploy, either manually or from a CI pipeline, it is advised to use a version control system 
like git. Here is a simple directory structure that can be used as a template for jobs and datastore resources, 
assuming there are 2 namespaces in a project.

```
.
├── optimus.yaml
├── README.md
├── namespace-1
│   ├── jobs
|   │   ├── job1
|   │   ├── job2
|   │   └── this.yaml
│   └── resources
|       ├── bigquery
│       │   ├── table1
│       │   ├── table2
|       |   └── this.yaml
│       └── postgres
│           └── table1
└── namespace-2
└── jobs
└── resources
```


You might have also noticed there are `this.yaml` files being used in some directories. This file is used to share a 
single set of configurations across multiple sub-directories. For example, if you create a file at 
/namespace-1/jobs/this.yaml, then all subdirectories inside /namespaces-1/jobs will inherit this config as defaults. 
If the same config is specified in subdirectory, then subdirectory will override the parent defaults.

For example a this.yaml in `/namespace-1/jobs`
```yaml
version: 1
schedule:
  interval: @daily
task:
  name: bq2bq
  config:
    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"
behavior:
  depends_on_past: false
  catch_up: true
  retry:
    count: 1
    delay: 5s
```

and a job.yaml in `/namespace-1/jobs/job1`
```yaml
name: sample_replace
owner: optimus@example.io
schedule:
  start_date: "2020-09-25"
  interval: 0 10 * * *
behavior:
  depends_on_past: true
task:
  name: bq2bq
  config:
    project: project_name
    dataset: project_dataset
    table: sample_replace
    load_method: REPLACE
window:
  size: 48h
  offset: 24h
```

will result in final computed job.yaml during deployment as
```yaml
version: 1
name: sample_replace
owner: optimus@example.io
schedule:
  start_date: "2020-10-06"
  interval: 0 10 * * *
behavior:
  depends_on_past: true
  catch_up: true
  retry:
    count: 1
    delay: 5s
task:
  name: bq2bq
  config:
    project: project_name
    dataset: project_dataset
    table: sample_replace
    load_method: REPLACE
    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"
window:
  size: 48h
  offset: 24h
```
