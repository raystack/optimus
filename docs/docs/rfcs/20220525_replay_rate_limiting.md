- Feature Name: Replay Rate Limit
- Status: Draft
- Start Date: 2022-05-25
- Authors: Arinda

# Summary

Replay is using the same scheduler pool and executor slots as the scheduled jobs. Current Replay is predicted to be 
causing issues and impacting the scheduled jobs, which might impacting the SLA of users. There should be a mechanism to 
use a different pool and slots and configurable by the users so any backfilling will not impact and causing delay of 
the scheduled jobs.

# Technical Design

## Background :

Currently, for bq2bq task the job is being executed using the project that specified in EXECUTION_PROJECT. This is also 
applied when the jobs are run through replay command. How Replay works is by clearing runs of the requested jobs and its 
dependency (if required), and there is no difference between the process of scheduled/manual job and replay. This also 
means the execution project and scheduler pool is also the same.


## Approach :

### Reduce the load of Execution Project
* There will be `REPLAY_EXECUTION_PROJECT` configuration, it can be set through the task config, project or namespace config.
* When building instances, Optimus will check if there is a replay request for the particular job and date, if yes, 
`EXECUTION_PROJECT` will be replaced with `REPLAY_EXECUTION_PROJECT`.

In job specs, task level:
```yaml
task:
  name: bq2bq
  config:
    PROJECT: sample-project
    DATASET: sample_dataset
    TABLE: sample_table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
    EXECUTION_PROJECT: main-executor-project
    REPLAY_EXECUTION_PROJECT: replay-executor-project
```


### Reduce the load of Airflow Default Pool
Initially, there are 3 mechanism to be considered for this.
1. Differentiating Airflow pool for Replay.
2. Trigger a new run for Replay (not using current clear method).
3. Limit the slot through Optimus.

**For the first option**, Airflow pool is configured through DAG. This approach requires task to be configured using 
each of the expected pool, for example `bq2bq-default` and `bq2bq-replay`. The task to be selected is being decided 
using `BranchPythonOperator`. This is not preferred as there will numerous task being set and visualize, and might 
confuse users.

**For the second option**, the replay run is being triggered using API, not by using the clear run method. However, 
Airflow is not accepting a pool configuration, thus it will still goes to the same pool.

**The third option**, is limiting the slot through Optimus. There will be configuration in project level to set the 
number of slots Replay can occupy.

Project configuration
```yaml
project:
  name: sample-project
  config:
    storage_path: gs://sample-bucket
    max_replay_runs_per_project: 15
    max_replay_runs_per_dag: 5
```

Lets say there is only 15 Replay slots at a time, and there are 40 runs requests comes. Replay will clear the first 15, 
put the rest 25 in queue, and Replay will keep checking on some interval, of whether the process has been completed and 
can take another runs.

Picking which runs to be processed:
* Based on Replay request time (FIFO)
* Based on the slot per dag. If the overall is set to 15 but the slot per dag is only 5, then a request to replay a job 
that has 10 runs cannot be done in one go.

There should be a default value for each of the configuration.
* Max_replay_runs_per_project: default 10
* Max_replay_runs_per_dag: default 5

Optimus will check on how many Replay job is currently running, and how many task run is running for each job. Additional 
changes might be needed on Replay side for optimization, to avoid calling Airflow API in every interval (Replay syncer 
responsibility) and fetch data from Replay table instead.
