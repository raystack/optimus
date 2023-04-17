# Managing Secrets
During job execution, specific credentials are needed to access required resources, for example, BigQuery credential 
for BQ to BQ tasks. Users are able to register secrets on their own, manage them, and use them in tasks and hooks. 
Please go through [concepts](../concepts/secret.md) to know more about secrets.

Before we begin, let’s take a look at several mandatory secrets that is used for specific use cases in Optimus.

| Secret Name        | Description                                                                                                                                                                                 |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| STORAGE            | To store compiled jobs if needed.                                                                                                                                                           |
| SCHEDULER_AUTH     | Scheduler credentials. For now, since Optimus only supports Airflow, this will be Airflow [username:password]                                                                               |
| BQ_SERVICE_ACCOUNT | Used for any operations involving BigQuery, such as job validation, deployment, run for jobs with BQ to BQ transformation task, as well as for managing BigQuery resources through Optimus. |


## Registering secret
Register a secret by running the following command:
```shell
$ optimus secret set someSecret someSecretValue
```

By default, Optimus will encode the secret value. However, to register a secret that has been encoded, run the following 
command instead:
```shell
$ optimus secret set someSecret encodedSecretValue --base64
```

There is also a flexibility to register using an existing secret file, instead of providing the secret value in the command.
```shell
$ optimus secret set someSecret --file=/path/to/secret
```

Secret can also be set to a specific namespace which can only be used by the jobs/resources in the namespace. 
To register, run the following command:
```shell
$ optimus secret set someSecret someSecretValue --namespace someNamespace
````

Please note that registering a secret that already exists will result in an error. Modifying an existing secret 
can be done using the Update command.

## Updating a secret
The update-only flag is generally used when you explicitly only want to update a secret that already exists and doesn't want to create it by mistake.
```shell
$ optimus secret set someSecret someSecretValue --update-only
```

It will return an error if the secret to update does not exist already.


## Listing secrets
The list command can be used to show the user-defined secrets which are registered with Optimus. It will list the namespace associated with a secret.
```shell
$ optimus secret list
Secrets for project: optimus-local
NAME    |                    DIGEST                    | NAMESPACE |         DATE
-------------+----------------------------------------------+-----------+----------------------
secret1   | SIBzsgUuHnExBY4qSzqcrlrb+3zCAHGu/4Fv1O8eMI8= |     *     | 2022-04-12T04:30:45Z
```

It shows a digest for the encrypted secret, so as not to send the cleartext password on the network.
