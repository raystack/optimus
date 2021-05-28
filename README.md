# Optimus

Optimus helps your organization to build & manage data pipelines with ease.

## Features
- BigQuery
    - Schedule BigQuery transformation
    - Query compile time templating (variables, loop, if statements, macros, etc)
    - Table creation
    - BigQuery View creation
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
- Extensibility to support Python transformation **[in roadmap]**
- Task versioning: If there is a scheduled job *A* and this gets modified as
  *A1* then it is possible to schedule same job for a date range as *A* and
  thereafter as *A1*. **[in roadmap]**
- Git based specification management
- HTTP/GRPC based specification management

### Compiling from source
Optimus requires the following dependencies:
* Golang (version 1.12 or above)

We won't cover how to install these, you can go to their respective websites to figure out how to install them for your OS.
Run the following commands to compile from source
```bash
$ git clone git@github.com:odpf/optimus.git
$ cd optimus
$ make
$ cp opctl /usr/bin # copy the executables to a location in $PATH
```
The last step isn't necessarily required. Feel free to put the compiled executable anywhere you want.
If during compilation, golang is unable to find odpf.github.io dependencies, try using

Note: building from source requires `buf` and `protoc-gen-go` binaries to be available in your shell path. If not found, you
can add following lines to your ~/.bashrc or ~/.zshrc.
```bash
export GOPATH=$(go env GOPATH)
export PATH=$PATH:$GOPATH/bin
```

## How to run web service

Follow same steps as optimus cli to compile from source
```bash
$ git clone git@github.com:odpf/optimus.git
$ cd optimus
$ make
```

Use the following command as an example
```bash
./optimus
```

### Service configuration

Configuration inputs can either be passed as command arguments or set as environment variable

| command                | env name               | required | description                                                       |
| ---------------------- | ---------------------- | -------- | ----------------------------------------------------------------- |
| server-port            | SERVER_PORT            | N        | port on which service will listen for http calls, default. `8080` |
| log-level              | LOG_LEVEL              | N        | log level - DEBUG, INFO, WARNING, ERROR, FATAL                    |
| ingress-host           | INGRESS_HOST           | Y        |                                                                   |
| db-host                | DB_HOST                | Y        |                                                                   |
| db-name                | DB_NAME                | Y        |                                                                   |
| db-user                | DB_USER                | Y        |                                                                   |
| db-password            | DB_PASSWORD            | Y        |                                                                   |

### To register a project as an entity
```
curl -X POST "optimus.example.io/api/v1/project" -H "accept: application/json" -H "Content-Type: 
application/json" -d "{ \"project\": { \"name\": \"project-name\", \"config\": { \"ENVIRONMENT\": \"integration\", 
\"STORAGE_PATH\": \"gs://bucket-path\" } }}"
```
Minimum basic configs required for optimus to work
- STORAGE_PATH: path of an object store to keep compiled jobs
- SCHEDULER_HOST: hostname of the scheduler for interacting with APIs

Execution unit configs which will be exposed as globals
- TRANSPORTER_KAFKA_BROKERS
- TRANSPORTER_STENCIL_HOST
- PREDATOR_HOST