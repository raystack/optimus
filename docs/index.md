# Optimus Documentation

Welcome to optimus documentation. We're still a work in progress, 
so expect more documentation soon!

Optimus was built to supersede bi_apps and bi-airflow, the current stack that powers
GOJEK's data warehouse. It started off originally as a scaffolding tool, 
but has grown beyond that and handles things like bigquery table management, 
dependency resolution. This document describes the concepts that Optimus works 
with, so that you can have a better understanding of how it's supposed to be used.

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
  * [Whats new in Optimus v2](concepts/v1-to-v2.md)
* [FAQ](reference/FAQ.md)
* [Contributing](contribute/contributing.md)

### References
- [Functional Data Warehouse](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)