---
id: profiling-auditing
title: Profiling and Auditing Bigquery
---

# Profiling and Auditing BigQuery

To enable Profiler and Auditor (Predator), answer the related questions in Job specification.
Note: this is not available for public use at the moment

```bash
• $ optimus create job
? What is the job name? test_job
? Who is the Owner of this job? de@go-jek.com
? Specify the start date (YYYY-MM-DD) 2021-01-01
? Specify the interval (in crontab notation) @daily
? Enable profile for the destination table? true
? Enable audit for the destination table? true
? Filter expression for profiling? (empty for always do full scan profiling) 
event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'
? Specify the profile/audit result grouping field (empty to not group the result) __PARTITION__
? Choose the profiling mode complete
```

Configs:
- **Filter expression**: Expression is used as a where clause to restrict the number of rows to only profile the ones 
  that needed to be profiled. 
  Expression can be templated with: DSTART and DEND. These will be replaced with the window for which the current 
  transformation is getting executed. EXECUTION_TIME will be replaced with job execution time that is being 
  used by the transformation task. `__PARTITION__` represents the partitioning field of the table and the type of 
  partition. If it is a daily partition using field `event_timestamp`, then the macros is equal to date
  `event_timestamp`.
- **Group**: Represent the column on which the records will be grouped for profiling. Can be `__PARTITION__` or any other 
  field in the target table.
- **Mode**: Mode represents the profiling strategy used with the above configurations, it doesn’t affect the profile 
  results. `complete` means all the records in a given group are considered for profiling. ‘incremental’ only the newly added records for the given group are considered for profiling. This input is needed when DataQuality results are shown in UI.

Here is a sample DAG specification that has Predator enabled.
```yaml
version: 1
name: test_job
owner: de@go-jek.com
schedule:
  start_date: "2021-02-26"
  interval: 0 2 * * *
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: bq2bq
  config:
    DATASET: playground
    LOAD_METHOD: REPLACE
    PROJECT: gcp-project
    SQL_TYPE: STANDARD
    TABLE: hello_test_table
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies: []
hooks:
  - name: predator
    config:
      AUDIT_TIME: '{{.EXECUTION_TIME}}'
      BQ_DATASET: '{{.TASK__DATASET}}'
      BQ_PROJECT: '{{.TASK__PROJECT}}'
      BQ_TABLE: '{{.TASK__TABLE}}'
      FILTER: 'event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"'
      GROUP: __PARTITION__
      MODE: complete
      PREDATOR_URL: '{{.GLOBAL__PREDATOR_HOST}}'
      SUB_COMMAND: profile_audit
```

After the Job is created, create a Data Quality Spec of the particular table and 
place it in the Optimus jobs repository, inside the Predator directory. 
Detail of quality spec creation is available in Predator documentation.
