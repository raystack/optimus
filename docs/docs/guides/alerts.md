---
id: alerts
title: Alerts
---

User needs to be alerted on specific events on jobs:
* There are two specific events on which a users can configure alerts as of today, `sla_miss` and `failure`
* Add the below sample configuration in your jobspec to configure the alerts.

Sample configuration
```yaml
behavior:
  notify:
  - 'on': failure/sla_miss
    config :
      duration : 2h45m
    channels:
    - slack://#slack-channel or @team-group or user&gmail.com
``` 

* sla_miss expects a duration config expect users to provide the duration as string.
* slack alerting is supported now which users can configure to channel or team handle or a specific user.