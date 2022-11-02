
# Optimus Dev setup

## Setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `make apply`

Some optional variable you can set alongside with `make apply`
```sh
DAGS_PATH=                # default /tmp/colima/dags
OPTIMUS_PLUGINS_PATH=     # default /tmp/colima/plugins
OPTIMUS_PLUGINS_ARTIFACT= # default ./optimus-dummy-plugins.tar.gz
OPTIMUS_SERVE_PORT=       # default 9100
```

## Components
+ optimus server
+ optimus db (postgres)
+ airflow 
+ airflow db (postgres)

### Dag files and installed plugins location on your laptop
+ location of your dag files: `/tmp/colima/dags` or specified by `DAGS_PATH`
+ location of your plugins: `/tmp/colima/plugins` or specified by `OPTIMUS_PLUGINS_PATH`

### Mounting plugins
+ yaml plugin and binary plugin can be directly added to [plugins folder on your laptop](#dag-files-and-installed-plugins-location-on-your-laptop)
+ plugin in artifact form can be added by specifiying the path through variable `OPTIMUS_PLUGINS_ARTIFACT`. Reapply optimus helm by specifying the artifact path.

```sh
OPTIMUS_PLUGINS_ARTIFACT=/Users/../plugins.tar.gz \
make apply.optimus
```

### Connect to optimus db
+ `psql -h localhost -U optimus`


