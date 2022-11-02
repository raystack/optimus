# Job run context
Need to remove the old api from protos.
Need to move one api from runtime to job_run.
Need to create a new api with
 - resolve priority
        Question: Do we need to resolve only based on number of dependencies ? or job should have
        an importance level like - Low, Medium, Standard, High, Critical
 - Compile the job to a dag
        Suggestion: Convert instance data to system defined env variables
        Question: Can we make this api stateless, the current blocker is execution_time,
        if we can convert the usage of execution_time to scheduled_at then we can do it.
 Can be triggered independently of the dependency_resolution

Check if we can move get_window call to job B.C.
Need to check if we can use the data present in *_run tables to determine the runs for the sensor.
    Then we stop the reliance on the airflow for run information.

Deploy to Scheduler once all resolution are done via separate stage in CI.
-- ReplaceAll (deploy1) - Job BC (Sync) 1-2min
    upstream
-- Deploy2 -> Job_Run BC (Async) 4-5min
    priority


---------------
Glossary -
Resources:
    Source => Destination
Job:
    Upstream => Job => Downstream


Sync        -> Depends on server to finish the work, request will not return.
Async       -> We return a response from server, and we keep checking the status.
Sequential  -> The operations are done in some order
Concurrent  -> The operation need not be run in sequential order, does not guarantee parallel.
Parallel    -> The operation is running parallel with other operations


Do not use following terms:
Dependency - not clear on if it is upstream or downstream or refers to the resource.

----------------
New card ->
Create improvement card around the levels of urgency.