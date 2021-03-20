# What's new in v2 so far

- Binary is called `opctl` now instead of optimus.
- There are no dags, tasks anymore. Specifications are called `job` and there is
  only a single file for it.
- There are no `properties.cfg` file, all these configs goes in same job.yaml file.
- Jobs have a single `asset` folder where the query should go for BQ transformation.
- Task window configuration which was specified in `properties.cfg` earlier also goes in 
  `job.yaml`.
- Macros are now formatted differently and follows golang template conventions.
  Instead of simply calling them via the name, they need to be enclosed in curly braces.
  For example:
```sql
Select * from sometable where event_time < "{{.DSTART}}"
```
- Macro replacements:
  - `dstart`: `{{.DSTART}}`
  - `dend`: `{{.DEND}}`
  - `__execution_time__`: `{{.EXECUTION_TIME}}`
  - `__destination_table__`: `{{.JOB_DESTINATION}}`
  > **Note**: dstart and dend used to provide date of the window start/end whereas
  now these convert to timestamp of window start/end, for example: 
  `2021-02-10T10:00:00+00:00` <br>
  To get the same value as before, macro can be pared with a pipe function:
  `{{ .DSTART | Date }}` that is `2021-02-10` 
- `query.sql` and all other asset files now supports compile time functions 
  evaluations defined in golang [docs](https://golang.org/pkg/text/template/) 
  and [sprig](http://masterminds.github.io/sprig/) library.
- No more `USE_SPILLOVER` config in properties.cfg. If the transformation needs 
  to be idempotent which we suggest it should always be, `REPLACE` load method can
  be selected. The problem here is what if the `Select` query that is provided with
  `REPLACE` load method actually generated more than one partition to be replaced.
  To make sure optimus replaces correct partitions, it has two strategies now
  - Auto: User leaves the responsibility to optimus to figure out target partitions
    automatically. This is internally done by first executing the select query and
    storing it in a temporary table. Then a `Select` query on its partitioned column
    is used to find all the effected partitions. Once this is known, a `Merge` 
    statement is used to replace identified partitions and insert the `Select` query.
    Note that this will cause almost 1.5x the cost of a query to incur.
  - Custom partition filter: User will provide a condition that can be directly used
    in a `Merge` statement to delete existing partitions from the destination table.
    This is cheaper and faster, for example: `
    ```
    DATE(event_timestamp) >= "{{.DSTART|Date}}" AND DATE(event_timestamp) < "{{ .DEND|Date }}"
    ```
- New fields compare to v1
  - `description`: Description of the job
  - `labels`: Job specific labels which will be passed to tasks and hooks. This 
     can be used to track cost, find jobs created by optimus, etc.
  - `dependencies`: This no longer support custom time delta for now. There is a
    new schema to define this
```yaml
dependencies:
  job: jobname
```
  - `depends_on_past`: self-explanatory as opctl generates them, same as V1
  - `catch_up`: self-explanatory as opctl generates them, same as V1
  - `start_date`: self-explanatory as opctl generates them, same as V1
  - `end_date`: when the job should finish executing the schedule
  - `interval`: self-explanatory as opctl generates them, same as V1
  

