---
id: adding-hook
title: Adding hook to a Job
---

There might be a certain operations that you might want to run before or after the Job.
Please go through [concepts](../concepts/overview.md) to know more about it.

In order to add a hook to an existing Job, run the following command and answer the 
corresponding prompts:

```
$ ./optimus create hook
? Select a Job example_job
? Which hook to run? transporter
? Filter expression for extracting transformation rows? event_timestamp >= '{{.DSTART}}' 
  AND event_timestamp < '{{.DEND}}'
```

With the above prompt, we're adding the *transporter* hook *post* the execution of 
primary job. Filter expression configuration is specific to a transporter hook, 
and it might be different for other hooks.

After this, existing job.yaml file will get updated with the new hook config, and 
the job specification would look like:

```yaml
version: 1
name: example_job
owner: example@example.com
description: example job to demonstrate hook
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
    LOAD_METHOD: APPEND
    PROJECT: example
    SQL_TYPE: STANDARD
    TABLE: hello_table
  window:
    size: 24h
    offset: "0"
    truncate_to: d
labels:
  orchestrator: optimus
dependencies: []
hooks:
- name: transporter
  config:
    BQ_DATASET: '{{.TASK__DATASET}}' # inherited from task configs
    BQ_PROJECT: '{{.TASK__PROJECT}}'
    BQ_TABLE: '{{.TASK__TABLE}}'
    FILTER_EXPRESSION: 'event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"'
    KAFKA_TOPIC: optimus_example-data-hello_table
    PRODUCER_CONFIG_BOOTSTRAP_SERVERS: '{{.GLOBAL__TRANSPORTER_KAFKA_BROKERS}}'
    PROTO_SCHEMA: example.data.HelloTable
    STENCIL_URL: '{{.GLOBAL__TRANSPORTER_KAFKA_BROKERS}}' # will be defined as global config
```

Now to finish this, create a commit and push changes to target repository.
The gitlab pipeline is idempotent and hence Optimus will handle the new 
specifications accordingly.