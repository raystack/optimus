---
id: introduction
title: Introduction
---

# Optimus

We're still a work in progress, so expect more documentation soon!

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
- Git based specification management
- GRPC/REST based specification management

NOTE: This is still in early stages and very close to use for production.
We are taking feedback and making breaking changes based on user requirements.
