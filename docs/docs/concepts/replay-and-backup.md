# Replay & Backup
A job might need to be re-run (backfill) due to business requirement changes or other various reasons. Optimus provides 
an easy way to do this using Replay. Replay accepts which job and range of date to be updated, validates it, and re-runs 
the job tasks.

When validating, Optimus checks if there is any Replay with the same job and date currently running and also checks if 
the task scheduler instances are still running to avoid any duplication and conflicts.

After passing the validation checks, a Replay request will be created and will be processed by the workers based on the 
mode chosen (sequential/parallel). To re-run the tasks, Optimus clears the existing runs from the scheduler.

**Sequential (Default)**

![Sequential Mode Flow](/img/docs/ReplaySequential.png "SequentialMode")

**Parallel**

![Parallel Mode Flow](/img/docs/ReplayParallel.png "ParallelMode")

Optimus also provides a Backup feature to duplicate a resource that can be perfectly used before running Replay. Where 
the backup result will be located, and the expiry detail can be configured in the project configuration.
