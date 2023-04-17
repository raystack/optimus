# Server Configuration
See the server configuration example on config.sample.yaml.

| Configuration    | Description                                                                                                                                                                               |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Log              | Logging level & format configuration.                                                                                                                                                     |
| Serve            | Represents any configuration needed to start Optimus, such as port, host, DB details, and application key (for secrets encryption). |
| Scheduler        | Any scheduler-related configuration. Currently, Optimus only supports Airflow and has been set to default. |
| Telemetry        | Can be used for tracking and debugging using Jaeger. |
| Plugin           | Optimus will try to look for the plugin artifacts through this configuration. |
| Resource Manager | If your server has jobs that are dependent on other jobs in another server, you can add that external Optimus server host as a resource manager. |

_Note:_

Application key can be randomly generated using: 
```shell
head -c 50 /dev/random | base64
```
Just take the first 32 characters of the string.

