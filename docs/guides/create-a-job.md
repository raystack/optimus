# Using Optimus to create a Job

A Job is the fundamental execution unit of an Optimus data pipeline. 
It can be scheduled, configured and is always mapped to a single transformation type
(eg, BQ-to-BQ, GCS-to-BQ, etc). It can have dependencies over other jobs and should
only execute once the dependent job is successfully completed. 

A job can also be configured with Hooks as part of its lifecycle, which can be
triggered before or after the job. Please go through [concepts](../concepts/overview.md) 
to know more about it.

Let's start with a basic example. For our tutorial, we'll be creating a job that 
writes "hello YYYY-MM-DD" to a table every day at 3 am. We'll use BQ-to-BQ transformation type. 
For the purpose of this tutorial, we'll assume that the Google Cloud Project name 
is "example" & dataset is just called "data".


## Creating a Job

Open your terminal and create a new directory that will hold the specifications 
created by `optimus` (The CLI of Optimus). Once ready, you can run the following 
command and answer the corresponding prompts (do note that some prompts 
would be to select from options instead of input):

```
$ optimus create job
? What is the job name? example_job
? Who is the owner of this job? example@example.com
? Which task to run? bq2bq
? Specify the start date 2021-02-18
? Specify the interval (in crontab notation) 0 3 * * *
? Project ID: example
? Dataset Name: data
? Table Name: hello_table
? Load method to use on destination? APPEND
```

Note: The cron schedule of a Job is as per UTC timezone.
With the above prompt, we have created a Job with name example_job that writes to table `hello_table` 
every day at 3 AM UTC, with the load method APPEND (we'll come back to this later). 
The task `bq2bq` refers to "BigQuery to BigQuery" transformation. As you can notice, 
each Job is mapped with a specific table. This will create the following files:

```
.
└── example_job
    ├── assets
    │ └── query.sql
    └── job.yaml
```

You can now edit `query.sql` and write the SQL query in it. for example:

```bash
$ cat > example_job/assets/query.sql <<_EOF
select CONCAT("Hello, ", "{{.DEND}}") as message
_EOF
```

`{{.DEND}}` is a macro that is replaced with the current execution date (in YYYY-MM-DD format) 
of the task (note that this is the execution date of when the task was supposed to run, 
not when it actually runs). There's another corresponding macro for start date called `
{{.DSTART}}` the value of which is DEND minus the task window. If the task window is DAILY, 
DSTART is one day behind DEND, if the window is weekly, DSTART is 7 days before DEND. 
Do note the format of macros, these are as per [golang template](https://golang.org/pkg/text/template/).

What about the load method then? Load method specifies write disposition of the task. 
There are currently 3 configurations available:
- APPEND
- REPLACE
- MERGE

When the load method is set to APPEND new rows are inserted to the table/partition 
when the job runs, REPLACE will truncate the table/partition before writing new rows 
and MERGE is used when you want to use BigQuery DML/Scripts. Which load method you use depends 
on the nature of the transformation, however it's advised to use the REPLACE method 
with a partitioned table to keep your queries idempotent. Another alternative would 
be to use the MERGE load method with DML. Keeping queries idempotent helps backfilling data.

Finally, this is how our Job Specification will look like (example_job/job.yaml):
```yaml
version: 1
name: example_job
owner: example@example.com
schedule:
  start_date: "2021-02-18"
  interval: 0 3 * * *
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: bq2bq
  config:
    PROJECT: example
    DATASET: data
    TABLE: hello_table
    LOAD_METHOD: APPEND
    SQL_TYPE: STANDARD
  window:
    size: 24h
    offset: "0"
    truncate_to: d
labels:
   orchestrator: optimus
dependencies: []
hooks: []
```

Now you can finally push all the files in a git repository. Create a commit and 
push to repository which will initiate gitlab pipeline and apply all of your changes. 
In this case:

1. Table is migrated in BigQuery for above bq2bq task [TODO]
2. Compiles your job specifications as Airflow DAG definitions and upload them to 
   Google cloud storage (or any other configured store) that gets synced to airflow 
   (or any other configured scheduler) linked with this git repository.

Optimus also supports managing Job Specifications via APIs. We'll talk about this in other sections.
You have now successfully deployed your transformation job onto your infrastructure.