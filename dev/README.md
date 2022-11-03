
# Optimus Dev setup

## Setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `make apply`

Some optional variable you can set alongside with `make apply`
```sh
DAGS_PATH=                # default ./dags
OPTIMUS_PLUGINS_PATH=     # default ./plugins
OPTIMUS_SERVE_PORT=       # default 9100
```

## Components
+ optimus server
+ optimus db (postgres)
+ airflow 
+ airflow db (postgres)

### Dag files and installed plugins location on your laptop
+ location of your dag files: `./dags` or specified by `DAGS_PATH`
+ location of your plugins: `./plugins` or specified by `OPTIMUS_PLUGINS_PATH`

### Mounting plugins
+ yaml plugin and binary plugin can be directly added to [plugins folder on your laptop](#dag-files-and-installed-plugins-location-on-your-laptop)

### Load secrets
+ provide file contains key value pair of secret name and secret value
+ multiple secret separated by new line. eg:

```
BQ_SERVICE_ACCOUNT="ZXhhbXBsZQ=="
EXAMPLE="ZXhhbXBsZTI="
```

+ load the secrets by specifying that file
```sh
PROJECT=<project-name> \    # default project-a
HOST=<host> \               # default localhost:9100
./load_secrets.sh <secret-path>
```

### Connect to optimus db
+ `psql -h localhost -U optimus`


