# Intervals and Windows

When defining a new job, you need to define the interval at which it will be triggered.
This parameter can give you a precise value when the job is scheduled for execution but
only a rough estimate exactly when the job is executing. It is very common in a ETL 
pipeline to know when the job is exactly executing as well as for what time window
the current transformation will consume the data. Knowledge of what time window this
transformation is consuming data from the source will tell what previous windows
we have already consumed and we can skip those.

For example: <br>
We have a table with 2 colums

| name        | created_at  |
| ----------- | ---------  |
| Rick        | 2021-03-05 |
| Sanchez     | 2021-03-06 |
| Serious     | 2021-03-07 |
| Sam         | 2021-03-07 |

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
job requirement. For a daily executing job, it could be of 24 hours, but for hourly
executing job, it will be 1 hour. Similary a job might be consuming data for a week
or a month, so no matter what day we are executing the query we might need to shift the
window dates to the first day of the month as start and last day of the month as end.

Optimus allows input window to be customized via 3 configurations.
- **Size**: It's the length of the task window, 1 hour, 24 hour, 1 week. This will result
  into the difference between DSTART and DEND
- **Offset**: It is a possible usecase that the day transformation is executing, and the
  day ETL want to consume the data has some time difference. For example, even though job
  is executing today and in normal situations, it should be consuming data from yesterday to
  X hours before, there are cases when the input window needs to be shifted to few hours in past 
  or even future.
- **Truncate_to**: Window start and end may not always lies on exactly the day job wants them
  to be even if we use the above parameters. Sometimes window just needs to be aligned
  to a well-defined business window like month start to month end, or week start to weekend
  even though today is middle of the week. `Truncate_to` helps aligning the windows to
  exact business time windows.