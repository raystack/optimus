# Configuration
Client configuration holds the necessary information for connecting to the Optimus server as well as for specification 
creation. Optimus provides a way for you to initialize the client configuration by using the `init` command. Go to the 
directory where you want to have your Optimus specifications. Run the below command and answer the prompt questions:

```shell
$ optimus init

? What is the Optimus service host? localhost:9100
? What is the Optimus project name? sample_project
? What is the namespace name? sample_namespace
? What is the type of data store for this namespace? bigquery
? Do you want to add another namespace? No
Client config is initialized successfully
```


After running the init command, the Optimus client config will be configured. Along with it, the directories for the 
chosen namespaces, including the sub-directories for jobs and resources will be created with the following structure:
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
   
                
| Configuration  | Description                                     |
|----------------|-------------------------------------------------|
| Version        | Supports only version 1 at the moment.          |
| Log            | Logging level & format configuration            |
| Host           | Optimus server host                             |
| Project        | Chosen Optimus project name and configurations. | 
| Namespaces     | Namespaces that are owned by the project.       |

## Project
- Project name should be unique.
- Several configs are mandatory for job compilation and deployment use case:
  - **storage_path** config to store the job compilation result. A path can be anything, for example, a local directory 
    path or a Google Cloud Storage path.
  - **scheduler_host** being used for job execution and sensors.
  - Specific secrets might be needed for the above configs. Take a look at the detail [here](managing-secrets.md).
- You can put any other project configurations which can be used in job specifications.

## Namespaces
- Name should be unique in the project.
- You can put any namespace configurations which can be used in specifications.
- Job path needs to be properly set so Optimus CLI will able to find all of your job specifications to be processed.
- For datastore, currently Optimus only accepts `bigquery` datastore type and you need to set the specification path 
  for this. Also, there is an optional `backup` config map. Take a look at the backup guide section [here](backup-bigquery-resource.md) 
  to understand more about this.
