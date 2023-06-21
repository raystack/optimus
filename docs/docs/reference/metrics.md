# Metrics

## Job Change Metrics

| Name                             | Type    | Description                                          | Labels                                           |
|----------------------------------|---------|------------------------------------------------------|--------------------------------------------------|
| job_events_total                 | counter | Number of job changes attempt.                       | project, namespace, status                       |
| job_upload_total                 | counter | Number of jobs uploaded to scheduler.                | project, namespace, status                       |
| job_removal_total                | counter | Number of jobs removed from scheduler.               | project, namespace, status                       |
| job_namespace_migrations_total   | counter | Number of jobs migrated from a namespace to another. | project, namespace_source, namespace_destination |
| job_replace_all_duration_seconds | counter | Duration of job 'replace-all' process in seconds.    | project, namespace                               |
| job_refresh_duration_seconds     | counter | Duration of job 'refresh' process in seconds.        | project                                          |
| job_validation_duration_seconds  | counter | Duration of job 'validation' process in seconds.     | project, namespace                               |

## JobRun Metrics

| Name                         | Type    | Description                                                                                                           | Labels                                   |
|------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| jobrun_events_total          | counter | Number of jobrun events in a job broken by the status, e.g sla_miss, wait_upstream, in_progress, success, failed.     | project, namespace, job, status          |
| jobrun_sensor_events_total   | counter | Number of sensor run events broken by the event_type, e.g start, retry, success, fail.                                | project, namespace, event_type           |
| jobrun_task_events_total     | counter | Number of task run events for a given operator (task name) broken by the event_type, e.g start, retry, success, fail. | project, namespace, event_type, operator |
| jobrun_hook_events_total     | counter | Number of hook run events for a given operator (task name) broken by the event_type, e.g start, retry, success, fail. | project, namespace, event_type, operator |
| jobrun_replay_requests_total | counter | Number of replay requests for a single job.                                                                           | project, namespace, job, status          |
| jobrun_alerts_total          | counter | Number of the alerts triggered broken by the alert type.                                                              | project, namespace, type                 |

## Resource Metrics

| Name                                 | Type    | Description                                                                     | Labels                                           |
|--------------------------------------|---------|---------------------------------------------------------------------------------|--------------------------------------------------|
| resource_events_total                | counter | Number of resource change attempts broken down by the resource type and status. | project, namespace, datastore, type, status      |
| resource_namespace_migrations_total  | counter | Number of resources migrated from a namespace to another namespace.             | project, namespace_source, namespace_destination |
| resource_upload_all_duration_seconds | gauge   | Duration of uploading all resource specification in seconds.                    | project, namespace                               |
| resource_backup_requests_total       | counter | Number of backup requests for a single resource.                                | project, namespace, resource, status             |

## Tenant Metrics

| Name                 | Type    | Description                        | Labels                     |
|----------------------|---------|------------------------------------|----------------------------|
| secret_events_total  | counter | Number of secret change attempts.  | project, namespace, status |

## System Metrics

| Name                                | Type    | Description                                              | Labels |
|-------------------------------------|---------|----------------------------------------------------------|--------|
| application_heartbeat               | counter | Optimus server heartbeat pings.                          | -      |
| application_uptime_seconds          | gauge   | Seconds since the application started.                   | -      |
| notification_queue_total            | counter | Number of items queued in the notification channel.      | type   |
| notification_worker_batch_total     | counter | Number of worker executions in the notification channel. | type   |
| notification_worker_send_err_total  | counter | Number of events created and to be sent to writer.       | type   |
| publisher_kafka_events_queued_total | counter | Number of events queued to be published to kafka topic.  | -      |
