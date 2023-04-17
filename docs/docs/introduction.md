---
id: introduction
title: Introduction
---

# Optimus

Optimus is an ETL orchestration tool that helps manage data transformation jobs and manage warehouse resources. 
It enables you to transform your data by writing the transformation script and YAML configuration while Optimus handles 
the dependency, schedules it, and handles all other aspects of running transformation jobs at scale. Optimus also supports 
warehouse resource management (currently BigQuery), which enables you to create, update, and read BigQuery tables, views, and datasets.

![High Level Optimus Diagram](/img/docs/OptimusIntro.png "OptimusIntro")

Optimus was made to be extensible. Adding support for different kinds of sources/sinks and transformation executors 
can be done easily. If your organization has to setup & manage data pipelines that are complex with multiple sources, 
sinks & there are many team members managing them, then Optimus is the perfect tool for you.

## Multi-Tenancy Support

Optimus supports multi-tenancy. Each tenant manages their own jobs, resources, secrets, and configuration while optimus 
managing dependencies across tenants.

## Extensible

Optimus provides the flexibility to you to define how your transformation jobs should behave, which data source or 
warehouse sink you want to support, and what configurations you need from the users. This flexibility is addressed 
through [plugin](concepts/plugin.md). At the moment, we provide a [BigQuery to BigQuery task plugin](https://github.com/goto/transformers/tree/main/task/bq2bq), 
but you can write custom plugins such as Python transformations.

Also, in order to provide a unified command line experience of various tools, Optimus provides [extensions](client-guide/work-with-extension.md) 
support on client side through which you can extend the capabilities for example providing governance. 

## Automated Dependency Resolution

Optimus parses your data transformation queries and builds a dependency graph automatically without the user explicitly 
defining the same. The dependencies are managed across tenants, so teams doesn’t need to coordinate among themselves.

## In-Built Alerting

Always get notified when your job is not behaving as expected, on failures or on SLA misses. Optimus supports 
integrations with slack & pagerduty.

## Verification in Advance

Minimize job runtime issues by validating and inspecting jobs before submitting them which enables them for faster 
turnaround time when submitting their jobs. Users can get to know about job dependencies, validation failures & some 
warnings before submitting the jobs.
