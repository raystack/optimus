- Feature Name: Optimus Dashboarding
- Status: Draft
- Start Date: 2022-05-17
- Authors: Yash Bhardwaj

# Summary

Optimus users need to frequestly visit airflow webserver which does not provide a optimus perspective of the jobs i.e. is lacks the leniage view and project / namespace level clasification.

The proposal here is to capture job related events and state information via airflow event callbacks.

# Design

## Job Metrics dashboard (aka. Job sumaries page):
- Monitor jobs performance stats :
    - Duaration per optimus job task/sensor/hook.
    - Job completion Sla misses trend.
    - Job summaries.
- Jobs groupings into namespaces and projects.
  - TODO: add Views and expectation from this page
- Check failues :
    - Failues per Optimus entity.
    - With links to the actual airflow webserver pages , explaining those failures further.

---

## Job lineage view :
- Currently only columbus provides lineage view.
- That does not reflect the job status and run level dependencies/relationships.
<!-- TODO add image to the wireframe -->

## Approach : Collect info from Airflow events / callbacks :
1. Get `DAG` lifecycle events from scheduler through:
    * Airflow triggered Callbacks
      * `on_success_callback` Invoked when the task succeeds
      * `on_failure_callback` Invoked when the task fails
      * `sla_miss_callback` Invoked when a task misses its defined SLA ( SLA here is scheduling delay not to be concused with job completion delay )
      * `on_retry_callback` Invoked when the task is up for retry
    * Events fired by our Custom logic added into Sensors Class

2. Information from these events is then relayed to the optimus server. Optimus then writes this into
  * `Prometheus` : for summaries dashboarding 
  * `OptimusDB`  : for powering lineage views 
3. Reasons for choosing this approach
  * Yhis is less tightly coupled with the Current chosen Scheduler.
    * If later support is provided for more schedulers, exixting optimus data collection APIs and Optimus Data model can be reused to power our Frontend system.
4. Known limitations:
  * Since this is an event based architecture, collection of Job/DAG state info will be hampered  in cases of panic failures for instance DAG python code throwing uncaught exception.

## Other Considerations:
* contrary approached discussed 
  * Kubernetes Sidecar
    * SideCar lifecycle hooks start/end 
    * sideCar to pull details from scheduler/plugin containers and push same to optimus server
  * Pull Approach
    * Callbacks -> statsD to power job sumaries page
    * access airflow API directly from Optimus to power job details view
* Future considerations 
  * Support for Events fired by Executor :
      * it is expected that even optimus-task and hooks shall independently be able to emit events to optimus notify api. this should help with improved executor observability.

## Terminology: 
* `Task` Airflow task operator
* `Job` Optimus job
* `DAG` Airflow DAG