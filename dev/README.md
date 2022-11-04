
# Optimus Dev setup

## Setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `make apply`

Some optional variable you can set alongside with `make apply`
```sh
DAGS_PATH=          # default /tmp/colima/dags
OPTIMUS_SERVE_PORT= # default 9100
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

### Mounting plugins and load secrets
+ define plugin artifacts on `setup.yaml` under section `plugins`
+ define key value pair of secrets on `setup.yaml` under section `secrets`
+ `make _setup`

Some optional variable you can set alongside with `make _setup`
```sh
SETUP_FILE_PATH= # default ./setup.yaml
PROJECT=         # default project-a
HOST=            # default localhost:9100
```

### Connect to optimus db
+ `psql -h localhost -U optimus`


