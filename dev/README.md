
# Optimus Dev setup

## Setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `cd dev`
+ `make apply`
+ expose the port to local machine:
    + `kubectl port-forward svc/optimus-dev 9100:80`
    + `kubectl port-forward svc/airflow-webserver 8080:8080`

Some optional variable you can set alongside with `make apply`
```sh
DAGS_PATH=                # default /tmp/colima/dags
OPTIMUS_SERVE_PORT=       # default 9100
SETUP_FILE_PATH=          # default ./setup.yaml
```

## Components
+ optimus server
+ optimus db (postgres)
+ airflow 
+ airflow db (postgres)

### Dag file location on your laptop
+ `/tmp/colima/dags` or specified by `DAGS_PATH`

### Spinning up the project
+ `mkdir project-a`
+ `cd project-a`
+ `optimus init`
+ `optimus project register`
+ `optimus plugin sync`
+ then load the secret. [ref](#load-secrets)

### Mounting the plugins
+ define plugin artifacts on `setup.yaml` under section `plugins`
+ `make apply`. You can pass `SETUP_FILE_PATH` if the path is not the default one

### Load secrets
+ define key value pair of secrets on `setup.yaml` under section `secrets`
+ `make _load.secrets`
Some optional variable you can set alongside with `make _load.secrets`
```sh
SETUP_FILE_PATH= # default ./setup.yaml
PROJECT=         # default project-a
HOST=            # default localhost:9100
```

### Connect to optimus db
+ `psql -h localhost -U optimus`


