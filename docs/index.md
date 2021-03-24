# Optimus Documentation

Welcome to optimus documentation. We're still a work in progress, 
so expect more documentation soon!

Optimus was built to supersede bi_apps and bi-airflow, the current stack that powers
GOJEK's data warehouse. It started off originally as a scaffolding tool, 
but has grown beyond that and handles things like bigquery table management, 
dependency resolution. This document describes the concepts that Optimus works 
with, so that you can have a better understanding of how it's supposed to be used.

## Features
- BigQuery
  - Schedule BigQuery transformation
  - Query compile time templating (variables, loop, if statements, macros, etc)
  - Table creation
  - BigQuery View creation **[WIP]**
  - BigQuery UDF creation **[in roadmap]**
  - Audit/Profile BigQuery tables
  - Sink BigQuery tables to Kafka
  - Automatic dependency resolution: In BigQuery if a query references
    tables/views as source, jobs required to create these tables will be added
    as dependencies automatically and optimus will wait for them to finish first.
  - Cross tenant dependency: Optimus is a multi-tenant service, if there are two
    tenants registered, serviceA and serviceB then service B can write queries
    referencing serviceA as source and Optimus will handle this dependency as well
  - Dry run query: Before SQL query is scheduled for transformation, during 
    deployment query will be dry-run to make sure it passes basic sanity 
    checks **[WIP]**
- Extensibility to support Python transformation **[in roadmap]**
- Extensibility to support Spark transformations **[in roadmap]**
- Task versioning: If there is a scheduled job *A* and this gets modified as
  *A1* then it is possible to schedule same job for a date range as *A* and 
  thereafter as *A1*. **[in roadmap]**
- Git based specification management
- HTTP/GRPC based specification management **[in roadmap]**
  
NOTE: This is still in early stages and very close to use for production.
We are taking feedback and making breaking changes based on user requirements.


# Table of Contents:
* [Installation](installation.md)
* Guides
  * Quickstart
    * [Creating a job](guides/creating-a-job.md)
    * [Adding a hook](guides/adding-a-hook.md)
  * Tasks
    * [BigQuery to BigQuery Transformation](guides/task-bq2bq.md)
  * Hooks
    * [Publishing BigQuery to Kafka](guides/publishing-from-bigquery-to-kafka.md)
    * [Profiling and Auditing BigQuery tables](guides/predator.md)
* [Concepts](concepts/index.md)
  * [Intervals and Windows](concepts/intervals-and-windows.md)
  * [Asset templating](concepts.md#Assets)
* [Whats new in Optimus v2](concepts/v1-to-v2.md)
* [FAQ](reference/FAQ.md)
* [Contributing](contribute/contributing.md)

### References
- [Functional Data Warehouse](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)
