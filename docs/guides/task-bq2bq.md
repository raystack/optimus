# BQ2BQ Task

### Creating Task
Command to create a task :
```
opctl create job
```
This command will invoke an interactive cli that contains configurations that 
need to be filled for the task. The tasks files will be generated at 
`{PWD}/jobs/{JOB_NAME}/assets` folder. 

Inside the assets folder there could be several files, one that is 
needed to configure this task is :

* query.sql - file that contains the transformation query

This will also configure the `job.yaml` with few defaults and few inputs requested at the time
of creation. User still able to change the config values after the file is generated.

For example `job.yaml` config :

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
    LOAD_METHOD: APPEND
    PROJECT: example
    SQL_TYPE: STANDARD
    TABLE: hello_table
  window:
    size: 24h
    offset: "0"
    truncate_to: d
```

Here are the details of each configuration and the allowed values :

| Config Name             | Description                                                                                                     | Values                              |
| ----------------------- |-----------------------------------------------------------------------------------------------------------------| ------------------------------------|
| `PROJECT`               | google cloud platform project id of the destination bigquery table                                              | ...                                 |
| `DATASET`               | bigquery dataset name of the destination table                                                                  | ...                                 |
| `TABLE`                 | the table name of the destination table                                                                         | ...                                 |
| `LOAD_METHOD`           | method to load data to the destination tables                                                                   | APPEND, REPLACE, MERGE              |
| `PARTITION_FILTER`      | Used to identify target partitions to replace in a REPLACE query. This can be left empty and optimus will figure the target partitions automatically but its cheaper and faster to specify the condition. This filter will be used as a where clause in a merge statement to delete the partitions from the destination table. | event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"      |

### Load Method

The way data loaded to destination table depends on the partition configuration of the destination tables

| Load Method  | No Partition                                                                                   | Partitioned Table                                                                          |
| -------------|------------------------------------------------------------------------------------------------| -------------------------------------------------------------------------------------------|
| APPEND       | Append new records to destination table                                                        | Append new records to destination table per partition based on localised start_time        |
| REPLACE      | Truncate/Clean the table before insert new records                                             | Clean records in destination partition before insert new record to new partition           |
| MERGE        | Load the data using DML Merge statement, all of the load logic lies on DML merge statement     | Load the data using DML Merge statement, all of the load logic lies on DML merge statement |

## query.sql file

The *query.sql* file contains transformation logic

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '{{.DSTART}}' and date(booking_creation_time) < '{{.DEND}}'
group by dt
```

### SQL macros

Macros is special variables in SQL that will be replaced by actual values when transformation executed

There are several SQL macros available

* {{.DSTART}} - start date/datetime of the window
* {{.DEND}} - end date/datetime of the window
* {{.JOB_DESTINATION}} - full qualified table name used in DML statement

The value of `DSTART` and `DEND` depends on `window` config in `job.yaml`. This is very similar to Optimus v1

| Window config                       | DSTART                                                             | DEND
| ----------------------------------- |--------------------------------------------------------------------| ---------------------------------------------------------------------|
| size:24h, offset:0, truncate_to:d   | The current date taken from input, for example 2019-01-01          | The next day after DSTART date 2019-01-02                            |
| size:168h, offset:0, truncate_to:w  | Start of the week date for example : 2019-04-01                    | End date of the week , for example : 2019-04-07                      |
| size:1M, offset:0, truncate_to:M    | Start of the month date, example : 2019-01-01                      | End date of the month, for example : 2019-01-31                      |
| size:2h, offset:0, truncate_to:h    | Datetime of the start of the hour, for example 2019-01-01 01:00:00 | Datetime the start of the next hour, for example 2019-01-01 02:00:00 |

Please find more details under [concepts](../concepts/intervals-and-windows.md) section.

Macros in SQL transformation example :

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '{{.DSTART}}' and date(booking_creation_time) < '{{.DEND}}'
group by dt
```

Rendered SQL for DAILY window example :

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '2019-01-01' and date(booking_creation_time) < '2019-01-02'
group by dt
```

Rendered SQL for HOURLY window example :
the value of `DSTART` and `DEND` is YYYY-mm-dd HH:MM:SS formatted datetime 

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '2019-01-01 06:00:00' and date(booking_creation_time) < '2019-01-01 07:00:00'
group by dt
```

destination_table macros example :

```sql
MERGE `{{.JOB_DESTINATION}}` S
using
(
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '{{.DSTART}}' and date(created_time) < '{{.DEND}}'
group by dt
) N
on S.date = N.date
WHEN MATCHED then
UPDATE SET `count` = N.count
when not matched then
INSERT (`date`, `count`) VALUES(N.date, N.count)
```

## SQL Helpers

Sometimes default behaviour of how tasks are being understood by optimus is not ideal. You can change this using helpers inside the query.sql file. To use, simply add them inside sql multiline comments where it’s required.
At the moment there is only one sql helper:

- `@ignoreupstream`: By default, Optimus adds all the external tables used inside the query file as its upstream 
dependency. This helper can help ignore unwanted waits for upstream dependency to finish before the current transformation can be executed.
Helper needs to be added just before the external table name. For example:
```sql
select
hakai,
rasengan,
`over`,
load_timestamp as `event_timestamp`
from /* @ignoreupstream */
`g-data-gojek-id-standardized.playground.sample_select`
WHERE
DATE(`load_timestamp`) >= DATE('dstart')
AND DATE(`load_timestamp`) < DATE('dend')
```

