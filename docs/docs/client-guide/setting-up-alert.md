# Setting up Alert to Job

There are chances that your job is failing due to some reason or missed the SLA. For these cases, you might want to set 
the alerts and get notified as soon as possible.

## Supported Events 

| Event Type | Description                                                                                                                                                      |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| failure    | Triggered when job run status is failed.                                                                                                                         |
| sla_miss   | Triggered when the job run does not complete within the duration that you expected. Duration should be specified in the config and should be in string duration. |


## Supported Channels

| Channel   | Description                                                                                 |
|-----------|---------------------------------------------------------------------------------------------|
| Slack     | Channel/team handle or specific user                                                        |
| Pagerduty | Needing `notify_<pagerduty_service_name>` secret with pagerduty integration key/routing key |


## Sample Configuration

```yaml
behavior:
notify:
- 'on': failure/sla_miss
  config :
    duration : 2h45m
  channels:
    - slack://#slack-channel or @team-group or user&gmail.com
    - pagerduty://#pagerduty_service_name
```


