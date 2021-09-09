# Airflow v2.x

Currently, allows configuring dags to be loaded from
- GCS bucket
- Local filesystem
- inmemory
implementation already supports variety of other systems, just need to configure them.
  
For using a fs that needs auth, it is required to create a project secret with
`STORAGE` as key and base64 encoded service account/token as value.

Optimus also provides api to get currently running job status using airflow APIs.
For this to work, it is required to register a secret with `SCHEDULER_AUTH` as key and
base64 encoded `username:password` as token. This assumes airflow is configured
to use basic auth on api by default.