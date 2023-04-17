# Replay a Job (Backfill)
Some old dates of a job might need to be re-run (backfill) due to business requirement changes, corrupt data, or other 
various reasons. Optimus provides a way to do this using Replay. Please go through [concepts](../concepts/replay-and-backup.md) to know more about it.

## Run a replay
To run a replay, run the following command:
```shell
$ optimus replay create {job_name} {start_time} {end_time} [flags]
```

Example:
```shell
$ optimus replay create sample-job 2023-03-01T00:00:00Z 2023-03-02T15:00:00Z --parallel --project sample-project --namespace-name sample-namespace
```

Replay accepts three arguments, first is the DAG name that is used in Optimus specification, second is the scheduled 
start time of replay, and third is the scheduled end time (optional) of replay.

Once your request has been successfully replayed, this means that Replay has cleared the requested runs in the scheduler. 
Please wait until the scheduler finishes scheduling and running those tasks.

## Get a replay status
You can check the replay status using the replay ID given previously and use in this command:
```shell
$ optimus replay status {replay_id} [flag]
```

You will see the latest replay status including the status of each run of your replay.

## Get list of replays
List of recent replay of a project can be checked using this sub command:
```shell
$ optimus replay list [flag]
```

Recent replay ID including the job, time window, replay time, and status will be shown. To check the detailed status 
of a replay, please use the status sub command.
