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
```
$ git clone git@github.com/odpf/optimus.git
$ cd optimus
$ make
$ cp opctl /usr/bin && cp optimus /usr/bin # copy the executables to a location in $PATH
```
The last step isn't necessarily required. Feel free to put the compiled executeable anywhere you want.
If during compilation, golang is unable to find odpf.github.io dependencies, try using
```
go env -w GOPRIVATE=odpf.github.io 
git config --global url."git@odpf.github.io:".insteadOf "https://odpf.github.io/"
```

## How to run web service

Follow same steps as optimus cli to compile from source
```
$ git clone git@github.com/odpf/optimus.git
$ cd optimus
$ make
```

Use the following command as an example
```
./optimus
```

### Service configuration

Configuration inputs can either be passed as command arguments or set as environment variable

| command                | env name               | required | description               |
| ---------------------- | ---------------------- | -------- | ------------------------- |
| server-port            | SERVER_PORT            | N        | port on which service will listen for http calls, default. `8080` |
| log-level              | LOG_LEVEL              | N        | log level - DEBUG, INFO, WARNING, ERROR, FATAL

You need to export GOOGLE_APPLICATION_CREDENTIALS with path to your service key, without this jazz cannot access GCS and GCR for deployment


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
