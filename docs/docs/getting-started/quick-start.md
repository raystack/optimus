# Quickstart
This quick start will guide you to try out Optimus fast without getting into many details. As part of this, you will 
be provided with step-by-step instructions to  start Optimus server, connect Optimus client with server, create 
BigQuery resource through Optimus, create BigQuery to BigQuery job, and deploy it.

## Prerequisite
- Docker or a local installation of Optimus.
- [Postgres](https://www.postgresql.org/download/) database.
- BigQuery project
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
  - This is not mandatory to complete the quick start, but needed for scheduling jobs.

## Step 1: Start Server
Start server with GOTO’s BigQuery to BigQuery [plugin](https://github.com/goto/transformers).

Create a config.yaml file:
```yaml
version: 1

log:
  level: debug

serve:
  port: 9100
  host: localhost
  ingress_host: localhost:9100
  app_key:
  db:
    dsn: postgres://<dbuser:dbpassword>@localhost:5432/dbname?sslmode=disable

plugin:
  artifacts:
   - https://github.com/goto/transformers/releases/download/v0.2.1/transformers_0.2.1_macos_x86_64.tar.gz
```
_Note: make sure you put artifacts link that suitable to your system._

### Start Server
With the config.yaml available in the same working directory, you can start server by running:
```shell
$ optimus serve --install-plugins
```

This will automatically install the plugins as specified in your server configuration.


## Step 2: Connect Client With Server
Go to the directory where you want to have your Optimus specifications. Create client configuration by using 
optimus `init` command. An interactive questionnaire will be presented, such as below:

```shell
$ optimus init

? What is the Optimus service host? localhost:9100
? What is the Optimus project name? sample_project
? What is the namespace name? sample_namespace
? What is the type of data store for this namespace? bigquery
? Do you want to add another namespace? No
Client config is initialized successfully
```


After running the init command, Optimus client config will be configured. Along with it, the directories for the chosen 
namespaces, including the subdirectories for jobs and resources will be created with the following structure:
```
sample_project
├── sample_namespace
│   └── jobs
│   └── resources
└── optimus.yaml
```

Below is the client configuration that has been generated:
```yaml
version: 1
log:
  level: INFO
  format: ""
host: localhost:9100
project:
  name: sample_project
  config: {}
namespaces:
- name: sample_namespace
  config: {}
  job:
    path: sample_namespace/jobs
  datastore:
    - type: bigquery
      path: sample_namespace/resources
      backup: {}
```

Let’s add `storage_path` project configuration that is needed to store the result of job compilation and 
`scheduler_host` which is needed for compilation.

```yaml
project:
  name: sample_project
  config:
    storage_path: file:///Users/sample_user/optimus/sample_project/compiled
    scheduler_host: http://sample-host
```

_Note: storage path is the location where airflow is reading its dags from._

Now, let's register `sample_project` and `sample_namespace` to your Optimus server.
```shell
$ optimus project register --with-namespaces
```


You can verify if the project has been registered successfully by running this command:
```shell
$ optimus project describe
```


## Step 3: Create BigQuery resource
Before creating BigQuery resources, make sure your Optimus server has access to your BQ project by adding a 
`BQ_SERVICE_ACCOUNT` secret.

Assume you have your service account json file in the same directory (project directory), create the secret using the 
following command. Make sure the service account that you are using is authorized to create tables.
```shell
$ optimus secret set BQ_SERVICE_ACCOUNT --file service_account.json
```


Check whether the secret has been registered successfully by running this command.
```shell
$ optimus secret list
```


Now, let’s create a resource using the following interactive command.
```shell
$ optimus resource create

? Please choose the namespace: sample_namespace
? What is the resource name? sample-project.sample_namespace.table1
? What is the resource type? table
? Provide new directory name to create for this spec? [sample_namespace/resources] sample-project.sample_namespace.table1

Resource spec [sample-project.sample_namespace.table1] is created successfully
```

_Note: resource name should be unique within the project. Take a look at the complete guide on how to create resource 
[here](../client-guide/manage-bigquery-resource.md) if needed._

After running the command, the resource specification file will be automatically created in the following directory:
```
sample_project
├── sample_namespace
│   └── jobs
│   └── resources
|       └── sample-project.sample_namespace.table1
|           └── resource.yaml
└── optimus.yaml
```


Let’s open the resource.yaml file and add additional spec details as follows:
```yaml
version: 1
name: sample-project.sample_namespace.table1
type: table
labels: {}
spec:
  description: "sample optimus quick start table"
  schema:
  - name: sample_day
    type: STRING
    mode: NULLABLE
  - name: sample_timestamp
    type: TIMESTAMP
    mode: NULLABLE
```

Now that resource specification is complete, let’s deploy this to the Optimus server and it will create the resource 
in BigQuery.

```shell
$ optimus resource upload-all --verbose

> Validating namespaces
namespace validation finished!

> Uploading all resources for namespaces [sample_namespace]
> Deploying bigquery resources for namespace [sample_namespace]
> Receiving responses:
[success] sample-project.sample_namespace.table1
resources with namespace [sample_namespace] are deployed successfully
finished uploading resource specifications to server!
```

## Step 4: Create & Deploy Job

Sync plugins to your local for optimus to provide an interactive UI to add jobs, this is a prerequisite before 
creating any jobs.
```shell
$ optimus plugin sync
```

Let’s verify if the plugin has been synced properly by running below command.
```shell
$ optimus version
```

You should find `bq2bq` plugin in the list of discovered plugins.

To create a job, we need to provide a job specification. Let’s create one using the interactive optimus job command.

```shell
$ optimus job create       
? Please choose the namespace: sample_namespace
? Provide new directory name to create for this spec? [.] sample-project.sample_namespace.table1
? What is the job name? sample-project.sample_namespace.table1
? Who is the owner of this job? sample_owner
? Select task to run? bq2bq
? Specify the schedule start date 2023-01-26
? Specify the schedule interval (in crontab notation) 0 2 * * *
? Transformation window daily
? Project ID sample-project
? Dataset Name sample_namespace
? Table ID table1
? Load method to use on destination REPLACE
Job successfully created at sample-project.sample_namespace.table1
```

_Note: take a look at the details of job creation [here](../client-guide/create-job-specifications.md)._

After running the job create command, the job specification file and assets directory are created in the following directory.
```
├── sample_namespace
│   └── jobs
|       └── sample-project.sample_namespace.table1
|           └── assets
|               └── query.sql
|           └── job.yaml
│   └── resources
|       └── sample-project.sample_namespace.table1
|           └── resource.yaml
└── optimus.yaml
```

For BQ2BQ job, the core transformation logic lies in `assets/query.sql`. Let’s modify the query to the following script:

```sql
SELECT
FORMAT_DATE('%A', CAST("{{ .DSTART }}" AS TIMESTAMP)) AS `sample_day`,
CAST("{{ .DSTART }}" AS TIMESTAMP) AS `sample_timestamp`;
```

_Note: take a look at Optimus’ supported macros [here](../concepts/macros.md)._

Let’s also verify the generated job.yaml file.
```yaml
version: 1
name: sample-project.sample_namespace.table1
owner: sample_owner
schedule:
  start_date: "2023-01-26"
  interval: 0 2 * * *
behavior:
  depends_on_past: false
task:
  name: bq2bq
  config:
    DATASET: sample_namespace
    LOAD_METHOD: REPLACE
    PROJECT: sample-project
    SQL_TYPE: STANDARD
    TABLE: table1
window:
  size: 24h
  offset: "0"
  truncate_to: d
labels:
  orchestrator: optimus
hooks: []
dependencies: []
```

For this quick start, we are not adding any hooks, dependencies, or alert configurations. Take a look at the details 
of job specification and the possible options [here](../client-guide/create-job-specifications.md#understanding-the-job-specifications).

Before proceeding, let’s add the BQ_SERVICE_ACCOUNT secret in the task configuration.
```yaml
task:
  name: bq2bq
  config:
    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"
    DATASET: sample_namespace
...
```

Later, you can avoid having the secret specified in every single job specification by adding it in the parent yaml 
specification instead. For more details, you can take a look [here](../client-guide/organizing-specifications.md).

Now the job specification has been prepared, lets try to add it to the server by running this command:
```shell
$ optimus job replace-all --verbose

> Validating namespaces
validation finished!

> Replacing all jobs for namespaces [sample_namespace]
> Receiving responses:
[sample_namespace] received 1 job specs
[sample_namespace] found 1 new, 0 modified, and 0 deleted job specs
[sample_namespace] processing job job1
[sample_namespace] successfully added 1 jobs
replace all job specifications finished!
```

Above command will try to add/modify all job specifications found in your project. We are not providing registering 
a single job through Optimus CLI, but it is possible to do so using API.

Now that the jobs has been registered to Optimus, let’s compile and upload it to the scheduler by using the following command.
```shell
$ optimus scheduler upload-all
```


The command will try to compile your job specification to the DAG file. The result will be stored in the `storage_path` 
location as you have specified when configuring the optimus.yaml file.

Later, once you have Airflow ready and want to try out, this directory can be used as a source to be scheduled by Airflow.
