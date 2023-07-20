# Installing Plugin in Client
Creating job specifications requires a plugin to be installed in the system caller. To simplify the installation, 
Optimus CLI can sync the YAML plugins supported and served by the Optimus server (with host as declared in project 
config) using the following command:

```shell
$ optimus plugin sync -c optimus.yaml
```

Note: This will install plugins in the `.plugins` folder.
