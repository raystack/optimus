
# Optimus Dev setup

## Setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `make apply`

Some optional variable you can set alongside with `make apply`
```sh
DAGS_PATH=                # default /tmp/colima/dags
OPTIMUS_SERVE_PORT=       # default 9100
OPTIMUS_PLUGIN_ARTIFACTS=
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
+ pass the variable OPTIMUS_PLUGIN_ARTIFACTS when [applying optimus](#setup) (comma separated delimiter). eg.
```sh
OPTIMUS_PLUGIN_ARTIFACTS=/User/.../path/to/plugin.tar.gz,http://github.com/.../example.yaml \
SETUP_FILE_PATH=./setup.yaml \
make apply
```

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


