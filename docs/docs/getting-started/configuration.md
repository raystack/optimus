---
id: configuration
title: Configurations
---

Optimus can be configured with `.optimus.yaml` file. An example of such is:
```yaml
version: 1

# used to connect optimus service
host: localhost:9100 

# project specification
project:
  
  # name of the Optimus project
  name: sample_project
  
  # project level variables usable in specifications
  config: {}

# namespace specification of the jobs and resources
namespace:
  
  # namespace name 
  name: sample_namespace
  
  jobs:
    # folder where job specifications are stored
    path: "job"
    
  datastore:
    # optimus is capable of supporting multiple datastores
    type: bigquery
    # path where resource spec for BQ are stored
    path: "bq"
    # backup configurations of a datastore
    backup:
      # backup result age until expired - default '720h'
      ttl: 168h
      # where backup result should be located - default 'optimus_backup'
      dataset: archive
      # backup result prefix table name - default 'backup'
      prefix: archive
    
  # namespace level variables usable in specifications
  config: {}

# for configuring optimus service locally
serve:
  
  # port to listen on
  port: 9100
  
  # host to listen on
  host: localhost
  
  # this gets injected in compiled dags to reach back out to optimus service
  # when they run
  ingress_host: optimus.example.io:80
  
  # 32 char hash used for encrypting secrets
  app_key: Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc
  
  # database configurations
  db:
    # database connection string
    dsn: postgres://user:password@localhost:5432/database?sslmode=disable
    
    max_idle_connection: 5
    max_open_connection: 10

# logging configuration
log:
  # debug, info, warning, error, fatal - default 'info'
  level: debug  

```

This configuration file should not be checked in version control. All the configs can also be passed as environment
variables using `OPTIMUS_<CONFIGNAME>` convention, for example to set client host `OPTIMUS_HOST=localhost:9100` to set
database connection string `OPTIMUS_SERVE_DB_DSN=postgres://dbconenctionurl`.

Assuming the following configuration layout:

```yaml
host: localhost:9100
serve:
  port: 9100
  app_key: randomhash
```

Key `host` can be set as an environment variable by upper-casing its path, using `_` as the
path delimiter and prefixing with `OPTIMUS_`:

`serve.port` -> `OPTIMUS_SERVE_PORT=9100`
or:
`host` -> `OPTIMUS_HOST=localhost:9100`

Environment variables always override values from the configuration file. Here are some more examples:

Configuration key | Environment variable |
------------------|----------------------|
host              | OPTIMUS_HOST         |
serve.app_key     | OPTIMUS_SERVE_APP_KEY|

App key is used to encrypt credentials and can be randomly generated using
```shell
head -c 50 /dev/random | base64
```
Just take the first 32 characters of the string.

Configuration file can be stored in following locations:
```shell
./
<exec>/
~/.optimus/
```