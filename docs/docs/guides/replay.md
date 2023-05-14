---
id: replay
title: Backfill jobs using Replay
---

Some old dates of a job might need to be re-run (backfill) due to business requirement changes, corrupt data, or other 
various reasons. Optimus provides an easy way to do this using Replay. Please go through 
[concepts](../concepts/overview.md) to know more about it.

## Run a replay

In order to run a replay, run the following command:

```shell
$ optimus replay create sample-job 2021-01-01 2021-02-01 --project sample-project --namespace sample-namespace
```

Replay accepts three arguments, first is the DAG name that is used in optimus specification, second is 
start date of replay, third is end date (optional) of replay.

If the replay request passes the basic validation, you will see all the tasks including the downstream that will be 
replayed. You can confirm to proceed to run replay if the run simulation is as expected.

Once your request has been successfully replayed, this means that Replay has cleared the mentioned task in the scheduler.
Please wait until the scheduler finishes scheduling and running those tasks. 

## Get a replay status

You can check the replay status using the replay ID given previously and use in this command:

```shell
$ optimus replay status {replay_id} --project sample-project
```

You will see the latest replay status including the status of each run of your replay.

## Get list of replays

List of recent replay of a project can be checked using this sub command:

```shell
$ optimus replay list --project sample-project
```

Recent replay ID including the job, time window, replay time, and status will be shown. To check the detailed status of a 
replay, please use the `status` sub command.

## Run a replay dry run

A dry run is also available to simulate all the impacted tasks without actually re-running the tasks. Example of dry run
usage:

```shell
$ optimus replay create sample-job 2021-01-01 2021-02-01 --project sample-project --namespace sample-namespace --dry-run
```
