### Communication between Opctl from a Job Run and Optimus

All tasks of a Job share some common metadata. In order to do so, Optimus store some metadata related to each JobRun into its database. Opctl makes call to those APIs to write and read data. Following are the steps that happens in this process:
- A DAG is scheduled on a scheduler with a specific scheduled_date
- when the first task is executed for this DAG, Opctl will:
    - send a RegisterJobRun request to Optimus
    - optimus will store the JobRun metadata, along with some values like DSTART, DEND, EXECUTION_TIME to be re-used by other tasks.
    - Opctl will then write the Job assets, env vars, config values into some files.
    - Job can now run
- when the next tasks for the same DAG start, Opctl will:
    - make a request to optimus which will retrieve all the values from the DB
    - Opctl will now write the same files etc which will be used by Job Run.
