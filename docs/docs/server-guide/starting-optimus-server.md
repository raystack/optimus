# Starting Optimus Server
Starting a server requires server configuration, which can be loaded from file (use --config flag), environment 
variable `OPTIMUS_[CONFIGNAME]`, or config.yaml file in Optimus binary directory.

**1. Using --config flag**
   ```shell
   $ optimus serve --config /path/to/config/file.yaml
   ```

If you specify the configuration file using the --config flag, then any configs defined in the env variable and default 
config.yaml from the Optimus binary directory will not be loaded.

**2. Using environment variable**

All the configs can be passed as environment variables using `OPTIMUS_[CONFIG_NAME]` convention. [CONFIG_NAME] is the 
key name of config using `_` as the path delimiter to concatenate between keys.

For example, to use the environment variable, assuming the following configuration layout:

```yaml
version: 1
serve:
  port: 9100
  app_key: randomhash
```


Here is the corresponding environment variable for the above

| Configuration key  | Environment variable  |
|--------------------|-----------------------|
| version            | OPTIMUS_VERSION       |
| serve.port         | OPTIMUS_PORT          |
| serve.app_key      | OPTIMUS_SERVE_APP_KEY |



Set the env variable using export
```shell
$ export OPTIMUS_PORT=9100
```


Note: If you specify the env variable and you also have config.yaml in the Optimus binary directory, then any configs 
from the env variable will override the configs defined in config.yaml in Optimus binary directory.


**3. Using default config.yaml from Optimus binary directory**
```shell
$ which optimus
/usr/local/bin/optimus
```

So the config.yaml file can be loaded on /usr/local/bin/config.yaml
