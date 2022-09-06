# Understanding Intervals and Windows

When defining a new job, you need to define the interval at which it will be triggered.
This parameter can give you a precise value when the job is scheduled for execution but
only a rough estimate exactly when the job is executing. It is very common in a ETL
pipeline to know when the job is exactly executing as well as for what time window
the current transformation will consume the data. Knowledge of what time window this
transformation is consuming data from the source will tell what previous windows
we have already consumed, and we can skip those.

For example: <br/>
We have a table with 2 columns

| name    | created_at |
| ------- | ---------- |
| Rick    | 2021-03-05 |
| Sanchez | 2021-03-06 |
| Serious | 2021-03-07 |
| Sam     | 2021-03-07 |

If we schedule a job for a cron interval of `0 2 * * *` that is daily at 2 AM, and start
the job from `2021-03-06`, on first day job will consume `Rick` as the input of the table
if we apply a filter over the SQL query of this table as

```sql
Select * from table where
created_at > DATE('{{.DSTART}}') AND
created_at <= DATE('{{.DEND}}')
```

Where `DSTART` could be replaced at the time of compilation with `2021-03-05` and
`DEND` with `2021-03-06`. This transformation will consume each row every day. Without
the provided filter, we will have to consume all the records which are created till date
inside the table even though the previous rows might already been processed.

These `DSTART` and `DEND` values of the input window could vary depending on the ETL
job requirement. 
- For a simple transformation job executing daily, it would need to consume full day worth of data of yesterday.
- A job might be consuming data for a week/month for an aggregation job, but the data boundaries should be complete,
  not consuming any partial data of a day.

Optimus allows user to define the amount of data window to consume through window configurations.
The configurations act on the schedule_time of the job and applied in order to compute dstart and dend.

- **Truncate_to**: The data window on most of the scenarios needs to be aligned to a well-defined time window 
  like month start to month end, or week start to weekend with week start being monday, or a complete day. 
  Inorder to achieve that the truncate_to option is provided which can be configured with either of these values 
  "h", "d", "w", "M" through which for a given schedule_time the end_time will be the end of last hour, day, week, month respectively.
- **Offset**: Offset is time duration configuration which enables user to move the `end_time` post truncation. 
  User can define the duration like "24h", "2h45m", "60s", "-45m24h", "0", "", "2M", "45M24h", "45M24h30m"
  where "h","m","s","M" means hour, month, seconds, Month respectively. 
- **Size**: Size enables user to define the amount of data to consume from the `end_time` again defined through the duration same as offset.

The above expectation of windowing is properly handled in job spec version 2, version 1 has some limitations in some of these expectations.
You can verify these configurations through `optimus playground command`
