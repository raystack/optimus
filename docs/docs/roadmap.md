---
id: roadmap
title: Roadmap
slug: /roadmap
---

This is a live document which gives an idea for all the users of Optimus on what are we upto. 

### Secret Management in Optimus
As optimus meant to deal with various warehouse systems & with the plugin support provides the capability to interact with other third party systems.
It brings a need for proper secret management to store & use securely for all users onboard.

### Test Your Jobs
Giving a provision for users to test the jobs before deploying helps users with faster feedbacks.

### Telemetry
With proper monitoring we can get many insights into Optimus, which helps in debugging any failures. May not be a direct end user feature but this is very important.  

### Add More Plugins
Once the data is analyzed in warehouse, there is always a need for getting the data out of the system for visualizations or for consumption. This is a constant effort to improve the ecosystem that optimus supports.
The plugins that we will be adding support is to pull data from BQ to Kafka, JDBC, FTP.

### Task Versioning
Versioning of tasks comes to handy when there is a time significance associated to task. 
On replay, an older version of task has to run which was active at that time and the newer version on the coming days.

### Improved Window Support
The current windowing which is used by for automated dependency resoultion & the macros which are derived from it are being used for input data filtering is little confusing & limiting in nature.
This will be an effort to easy the same.

### SLA Tracking
Giving a provision for defining the SLAs & providing a dashboard to visualize how the slas are met is a must.
With this users will be able to monitor any slas that are breached out of the box.

### SQLite Support
With support of SQLlite database, just helps users to kick start Optimus fast & easy to try on without having a dependency on postgres.

### Custom Macro Support
Custom Macros will unleash many capabilities, this will help users to template their queries to avoid any duplication.

### Inbuilt Testing F/w
Currently Optimus relies on Predator for Quality Checks, instead of relying on predator which is not extensible & supports only BQ, providing a capability to test the job runs directly.

### Inter Task/Hook Communication
As we scale, there are situations where tasks & hooks has to share information directly instead of relying on another system. This opens up many capabilities.
