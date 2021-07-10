---
id: organising-specifications
title: Organising specifications
---

Optimus supports two ways to deploy specifications
- REST/GRPC
- Optimus CLI deploy command

When using Optimus cli to deploy, either manually or from a CI pipeline, it is advised to use version control system like git. Here is a simple directory structure that can be used as a template for jobs and datastore resources.


```
.
├── .optimus.yaml
├── README.md
├── datastore
│   ├── bigquery
│   │   ├── project1
│   │   │   ├── dataset1
│   │   │   │   ├── table1
│   │   │   │   └── table2
│   │   │   └── this.yaml
│   │   └── project2
│   │       └── dataset1
│   │           └── table1
│   └── postgres
│       └── table1
└── jobs
    ├── project1
    │   ├── job1
    │   ├── job2
    │   └── this.yaml
    ├── project2
    │   ├── job1
    │   └── job2
    └── this.yaml
```



A sample `.optimus.yaml` would look like

```yaml
version: 1
host: localhost:9100
job:
  path: jobs
datastore:
- type: bigquery
  path: datastore/bigquery
- type: postgres
  path: datastore/postgres
config:
  global:
    environment: integration
    storage_path: gs://example-bucket/test    
  local: {}
```



You might have also noticed there are `this.yaml` files being used in some directories. This file is used to share a single set of configuration across multiple sub directories. For example if I create a file at `/jobs/project1/this.yaml`, then all sub directories inside `/jobs/project1` will inherit this config as defaults. If same config is specified in sub directory, then sub directory will override the parent defaults. For example a `this.yaml` in `/jobs/project/`

```yaml
version: 1
schedule:
  interval: @daily
behavior:
  depends_on_past: false
  catch_up: true
  retry:
    count: 1
    delay: 5s
labels:
  owner: overlords
  transform: sql
```



and a `job.yaml` in `/jobs/project/job1/`

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
labels:
  process: bq
```

will result in final computed `job.yaml` during deployment as

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
  window:
    size: 48h
    offset: 24h
labels:
  process: bq
  owner: overlords
  transform: sql
```

