
# Optimus Dev setup

## setup
+ `brew install colima`
+ `make start-colima`
+ check kubernetes context
+ `make apply`

## Components
+ optimus server
+ optimus db (postgres)
+ airflow 
+ airflow db (postgres)

### Dag file location on your laptop
+  `/tmp/colima/dags`

### Mounting plugins
+ update `path`'s for `internal-plugins` and `odpf-plugins` volumes in optimus.values.yaml with the actual path to the plugins and upgrade the helm chart for optimus (`make upgrade.optimus`)

```yaml
volumes:
  - name: internal-plugins
    hostPath: 
      path: /Users/../Documents/proj/optimus-plugins/dist/optimus-plugins_0.6.1_linux_amd64.tar.gz
      type: File
  - name: odpf-plugins
    hostPath: 
      path: /Users/../Documents/proj/transformers/dist/transformers_0.1.1_linux_arm64.tar.gz
      type: File
```

### Connect to optimus db
+ `psql -h localhost -U optimus`


