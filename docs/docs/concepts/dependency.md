# Dependency

A job can have a source and a destination to start with. This source could be a resource managed by Optimus or 
non-managed like an S3 bucket. If the dependency is managed by Optimus, it is obvious that in an ETL pipeline, it is 
required for the dependency to finish successfully first before the dependent job can start.

There are 2 types of dependency depending on how to configure it:

| Type      | Description                                                                                                                         |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------|
| Inferred  | Automatically detected through assets. The logic on how to detect the dependency is configured in each of the [plugins](plugin.md). |
| Static    | Configured through job.yaml                                                                                                         |

Optimus also supports job dependency to cross-optimus servers. These Optimus servers are considered external resource 
managers, where Optimus will look for the job sources that have not been resolved internally and create the dependency. 
These resource managers should be configured in the server configuration.
