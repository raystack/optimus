# Intervals and Windows

When defining a new job, you need to define the **interval (cron)** at which it will be triggered. This parameter can give 
you a precise value when the job is scheduled for execution but only a rough estimate exactly when the job is executing. 
It is very common in a ETL pipeline to know when the job is exactly executing as well as for what time window the current 
transformation will consume the data.

For example, assume there is a job that querying from a table using below statement:
```sql
SELECT * FROM table WHERE
created_at >= DATE('{{.DSTART}}') AND
created_at < DATE('{{.DEND}}')
```

**_DSTART_** and **_DEND_** could be replaced at the time of compilation with based on its window configuration. 
Without the provided filter, we will have to consume all the records which are created till date inside the table 
even though the previous rows might already been processed.

These _DSTART_ and _DEND_ values of the input window could vary depending on the ETL job requirement.
- For a simple transformation job executing daily, it would need to consume full day work of yesterday’s data.
- A job might be consuming data for a week/month for an aggregation job, but the data boundaries should be complete, 
  not consuming any partial data of a day.

## Window Configuration

Optimus allows user to define the amount of data window to consume through window configurations. The configurations 
act on the schedule_time of the job and applied in order to compute _DSTART_ and _DEND_.

- **Truncate_to**: The data window on most of the scenarios needs to be aligned to a well-defined time window
  like month start to month end, or week start to weekend with week start being monday, or a complete day.
  Inorder to achieve that the truncate_to option is provided which can be configured with either of these values
  "h", "d", "w", "M" through which for a given schedule_time the end_time will be the end of last hour, day, week, month respectively.
- **Offset**: Offset is time duration configuration which enables user to move the `end_time` post truncation.
  User can define the duration like "24h", "2h45m", "60s", "-45m24h", "0", "", "2M", "45M24h", "45M24h30m"
  where "h","m","s","M" means hour, month, seconds, Month respectively.
- **Size**: Size enables user to define the amount of data to consume from the `end_time` again defined through the duration same as offset.

For example, previous-mentioned job has `0 2 * * *` schedule interval and is scheduled to run on 
**2023-03-07 at 02.00 UTC** with following details:

| Configuration | Value | Description                                                                            |
|---------------|-------|----------------------------------------------------------------------------------------|
| Truncate_to   | d     | Even though it is scheduled at 02.00 AM, data window will be day-truncated (00.00 AM). |
| Offset        | -24h  | Shifts the window to be 1 day earlier.                                                 |
| Size          | 24h   | Gap between DSTART and DEND is 24h.                                                    |

Above configuration will produce below window:
- _DSTART_: 2023-04-05T00:00:00Z
- _DEND_: 2023-04-06T00:00:00Z

This means, the query will be compiled to the following query

```sql
SELECT * FROM table WHERE
created_at >= DATE('2023-04-05T00:00:00Z') AND
created_at < DATE('2023-04-06T00:00:00Z')
```

Assume the table content is as the following:

| name    | created_at |
| ------- |------------|
| Rick    | 2023-03-05 |
| Sanchez | 2023-03-06 |
| Serious | 2023-03-07 |
| Sam     | 2023-03-07 |

When the job that scheduled at **2023-03-07** runs, the job will consume `Rick` as the input of the table.

The above expectation of windowing is properly handled in job spec version 2, version 1 has some limitations in some of 
these expectations. You can verify these configurations by trying out in below command:
```
$ optimus playground
```
