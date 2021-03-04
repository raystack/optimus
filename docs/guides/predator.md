# Profiling and Auditing BigQuery tables
To enable Profiler and Auditor (Predator), answer the related questions in DAG specification.

```bash
• $ optimus create dag
? What is the DAG name? sample_replace
? Who is the Owner of this DAG? de@go-jek.com
? Which task to run? sample_replace
? Which environment will this DAG run in? staging
? Specify the start date (YYYY-MM-DD) 2021-01-01
? Specify the interval (in crontab notation) @daily
? Enable profile for the destination table? true
? Enable audit for the destination table? true
? Filter expression for profiling? (empty for always do full scan profiling) last_modified_timestamp = “__execution_time__”
? Specify the profile/audit result grouping field (empty to not group the result) __PARTITION__
? Choose the profiling mode complete
```

Below specified details need to be provided
Filter expression: Expression is used as a where clause to restrict the number of rows to only profile the ones that needed to be profiled. Expression can be templated with:
dstart and dend. These will be replaced with the window for which the current transformation is getting executed.
{{.EXECUTION_TIME}} will be replaced with job execution time that is being used by the transformation task.
__PARTITION__ represents the partitioning field of the table and the type of partition. If it is a daily partition using the field `event_timestamp`, then the macros is equal to date(event_timestamp).
Group: Represent the column on which the records will be grouped for profiling. Can be __PARTITION__ or any other field in the target table.
Mode: Mode represents the profiling strategy used with the above configurations, it doesn’t affect the profile results. `complete` means all the records in a given group are considered for profiling. ‘incremental’ only the newly added records for the given group are considered for profiling. This input is needed when DataQuality results are shown in UI.

Here is a sample DAG specification that has Predator enabled.
```yaml
owner: de@go-jek.com
environment: integration
spec:
startdate: "2021-01-01"
interval: '@daily'
depends_on_past: false
catchup: true
tasks:
- name: predator_sample_replace
  host: asia.gcr.io
  image: de-predatorcli:latest
  namespace: systems-0001
  env_vars:
  FILTER: last_modified_timestamp = “{{.EXECUTION_TIME}}”
  GROUP: __PARTITION__
  MODE: complete
  PREDATOR_URL: http://predator.test
  SUB_COMMAND: profile_audit
  AUDIT_TIME: "{{ ts }}"
  hook: true
  dependencies: []
```

After the DAG is created,  create a Data Quality Spec of the particular table and place it in the Optimus jobs repository, inside the Predator directory. Detail of quality spec creation is available in Predator documentation.
