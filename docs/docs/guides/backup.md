---
id: backup
title: Backup Resources
---

Backup is a common prerequisite step to be done before re-running or modifying a resource. Currently, Optimus supports 
backup for BigQuery tables and provides dependency resolution, so backup can be also done to all the downstream tables 
as long as it is registered in Optimus and within the same project.

## Configuring backup details

Several configurations can be set to have the backup result in your project as your preference. Here are the 
available configurations for BigQuery datastore.

Configuration key | Description                              | Default        |
------------------|------------------------------------------|----------------|
ttl               | Time to live in duration                 | 720h           |
prefix            | Prefix of the result table name          | backup         |
dataset           | Where the table result should be located | optimus_backup |

These values can be set in the project [configuration](../getting-started/configuration.md).


## Run a backup

To start a backup, run the following command:

```shell
$ optimus backup resource --project sample-project --namespace sample-namespace
```

After you run the command, prompts will be shown. You will need to answer the questions.

```
$ optimus backup resource --project sample-project --namespace sample-namespace
? Select supported datastore? bigquery
? Why is this backup needed? backfill due to business logic change
? Backup downstream? Yes
```

You will be shown a list of resources that will be backed up, including the downstream resources (if you chose to do so).
You can confirm to proceed if the list is as expected, and please wait until the backup is finished.

Once the backup is finished, the list of backup results along with where it is located will be shown.


## Get list of backups

List of recent backups of a project can be checked using this sub command:

```shell
$ optimus backup list --project sample-project
```

Recent backup ID including the resource, when it was created, what is the description or purpose of the backup will be 
shown. Backup ID is used as a postfix in backup result name, thus you can find those results in the datastore 
(for example BigQuery) using the backup ID. However, keep in mind that these backup results have expiry time set.

## Run a backup dry run

A dry run is also available to simulate all the resources that can be backed up without actually doing it. Example of dry 
run usage:

```shell
$ optimus backup resource --project sample-project --namespace sample-namespace --dry-run
```
