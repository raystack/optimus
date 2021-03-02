[![pipeline status](https://github.com/odpf/optimus/badges/master/pipeline.svg)](https://github.com/odpf/optimus/commits/master)
[![coverage report](https://github.com/odpf/optimus/badges/master/coverage.svg)](https://github.com/odpf/optimus/commits/master)

# Optimus

Optimus helps your organization to build & manage data pipelines with ease.

Some features of Optimus:
* Interactive CLI
* Automatic Dependency Resolution

### Compiling from source
Optimus requires the following dependencies:
* Golang (version 1.12 or above)

We won't cover how to install these, you can go to their respective websites to figure out how to install them for your OS.
Run the following commands to compile from source
```bash
$ git clone git@github.com/odpf/optimus.git
$ cd optimus
$ make
$ cp opctl /usr/bin && cp optimus /usr/bin # copy the executables to a location in $PATH
```
The last step isn't necessarily required. Feel free to put the compiled executeable anywhere you want.
If during compilation, golang is unable to find odpf.github.io dependencies, try using
```bash
go env -w GOPRIVATE=odpf.github.io 
git config --global url."git@odpf.github.io:".insteadOf "https://odpf.github.io/"
```

Note: building from source requires `buf` and `protoc-gen-go` binaries to be available in your shell path. If not found, you
can add following lines to your ~/.bashrc or ~/.zshrc.
```bash
export GOPATH=$(go env GOPATH)
export PATH=$PATH:$GOPATH/bin
```

## How to run web service

Follow same steps as optimus cli to compile from source
```bash
$ git clone git@github.com/odpf/optimus.git
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

You need to export GOOGLE_APPLICATION_CREDENTIALS with path to your service key, without this jazz cannot access GCS and GCR for deployment

### To register a project as an entity
```
curl -X POST "localhost/api/v1/project" -H "accept: application/json" -H "Content-Type: 
application/json" -d "{ \"project\": { \"name\": \"project-name\", \"config\": { \"ENVIRONMENT\": \"integration\", 
\"STORAGE_PATH\": \"gs://bucket-path\" } }}"
```
Minimum basic configs required for optimus to work
- STORAGE_PATH: path of an object store to keep compiled jobs
- SCHEDULER_HOST: hostname of the scheduler for interacting with APIs

Execution unit configs which will be exposed as globals
- TRANSPORTER_KAFKA_BROKERS  e.g. localhost:9092
- TRANSPORTER_STENCIL_HOST e.g. http://odpf/artifactory/proto-descriptors/ocean-proton/latest
- PREDATOR_HOST 

## Built With
* [Golang](https://golang.org/) - The Programming Language
* [Docker](https://www.docker.com/) - Tool for creating and running container images
* [Kubernetes](https://airflow.apache.org/kubernetes.html) - Docker container orchestration
* [Survey](https://github.com/AlecAivazis/survey) - A golang library for building interactive prompts
* [Cobra](https://github.com/spf13/cobra)- A Commander for modern Go CLI interactions
* [Airflow](https://github.com/apache/airflow) - Scheduler, workflow manager


## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/odpf/optimus/tags).


###### Have any feedbacks or want to contribute? Contact us at #data-engineering slack channel
