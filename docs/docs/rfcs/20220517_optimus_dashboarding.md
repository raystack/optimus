- Feature Name: Optimus Dashboarding
- Status: Draft
- Start Date: 2022-05-17
- Authors: Yash Bhardwaj

# Summary

Optimus users need to frequestly visit airflow webserver which does not provide a optimus perspective of the jobs i.e. is lacks the leniage view and project / namespace level clasification.

The proposal here is to capture job related events and state information via airflow event callbacks and job lifecycle events .

# Design

## Job Metrics dashboard (aka. Job sumaries page):
- Monitor jobs performance stats :
    - Duaration per optimus job task/sensor/hook.
    - Job completion Sla misses trend.
    - Job summaries.
- Jobs groupings into namespaces and projects.
<!-- - Check failues :
    - Failues per Optimus entity.
    - With links to the actual airflow webserver pages , explaining those failures further. -->

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
    * Events fired by our Custom logic added into 
      * Custom operator implimentation 
      * airflow job_start_event and job_end_event task

2. Information from these events is then relayed to the optimus server. Optimus then writes this into
  * `OptimusDB`  : for summaries dashboarding and for powering lineage views (in future)
3. Reasons for choosing this approach
  * This is less tightly coupled with the Current chosen Scheduler.
    * If later support is provided for more schedulers, exixting optimus data collection APIs and Optimus Data model can be reused to power our Frontend system.
4. Known limitations:
  * Since this is an event based architecture, collection of Job/DAG state info will be hampered  in cases of panic failures for instance DAG python code throwing uncaught exception.
    * even in such cases we will be able to determine the SLA missing jobs 
---

## Optimus Perspective DB model:
* Optimus shall consider each scheduled run and its subsiquent retry as a new job_run 
* sensor/task/hooks run information shall be grouped per job_run 
  * To achive this, each sensor/task/hook Run is linked with the job_run.id and job_run.attempt_number
  * while registering a sensor run the latest job_run.attempt for that given schedule time is used to link it.
* For each job_run(scheduled/re-run) there will only be one row per each sensor/task/hook registered in the Optimus DB.

---

## Compute SLA miss:

### Approach : 
  * Note Job Start Time
  * With Each Job Run, associate the then SLA definition(user defined number in the job.yaml) of the job.
  * SLA breach are determined with the read QUERY of Grafana, which works as following 
    * SLA breach duration = `(min(job_end_time , time.now()) - job_start_time) - SLA_definition`
  * Limitations
    * Since this is an event based data collection setup is it possible that a job may have failed/crashed/hangged in such a situation optimus wont get the job finish callback, and hence cant determine the SLA breach 
      * To work arround with that, at the time of job run registeration the end time is assumed to be a far future date. In case the job terminates properly we shall be able to determine the correct end_time , otherwise optimus is safe to assume that the job has not finised yet. The job Duration in such case will be the the time since the job has started running.

---
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