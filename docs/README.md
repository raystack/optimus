# Optimus Documentation

Welcome to optimus documentation. We're still a work in progress, 
so expect more documentation soon!

Optimus is an ETL orchestration tool that helps manage warehouse resources and 
schedule transformation over cron interval. Warehouses like Bigquery can be used
to create, update, read, delete different types of resources(dataset/table/standard view).
Similarly, jobs can be SQL transformations taking inputs from single/multiple
source tables executing over fixed schedule interval. Optimus was made from start
to be extensible, which is, adding support of different kind of warehouses, 
transformers can be done easily.

## Features
- BigQuery
  - Schedule BigQuery transformation
  - Query compile time templating (variables, loop, if statements, macros, etc)
  - BigQuery Dataset/Table/View creation
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
    checks
- Extensibility to support Python transformation
- Task versioning: If there is a scheduled job *A* and this gets modified as
  *A1* then it is possible to schedule same job for a date range as *A* and 
  thereafter as *A1*. **[in roadmap]**
- Git based specification management
- GRPC/REST based specification management **[in roadmap]**
  
NOTE: This is still in early stages and very close to use for production.
We are taking feedback and making breaking changes based on user requirements.


# Table of Contents:
* [Installation](installation.md)
* Guides
  * Quickstart
    * [Creating a job](guides/creating-a-job.md)
    * [Adding a hook](guides/adding-a-hook.md)
  * Transformer
    * [BigQuery to BigQuery Transformation](guides/task-bq2bq.md)
  * Hook
    * [Publishing BigQuery to Kafka](guides/publishing-from-bigquery-to-kafka.md)
    * [Profiling and Auditing BigQuery tables](guides/predator.md)
  * Datastore
    * [Create bigquery dataset](guides/create-bigquery-dataset.md)
    * [Create bigquery table](guides/create-bigquery-table.md)
    * [Create bigquery view](guides/create-bigquery-view.md)
* [Concepts](concepts/index.md)
  * [Intervals and Windows](concepts/intervals-and-windows.md)
  * [Asset templating](concepts/index.md#Assets)
* [Whats new in Optimus v2](concepts/v1-to-v2.md)
* [FAQ](reference/FAQ.md)
* [Contributing](contribute/contributing.md)

### References
- [REST API](https://github.com/odpf/optimus/blob/423da2b52f454d8ef8a4297873a3cf3d1fc9067a/third_party/OpenAPI/odpf/optimus/RuntimeService.swagger.json)
- [GRPC](https://github.com/odpf/proton/blob/c13453f190124e2d94a485343768b3f59b4da061/odpf/optimus/runtime_service.proto)
- [Functional Data Warehouse](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)
