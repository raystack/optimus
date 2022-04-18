---
id: secret
title: Manage Secrets
---

During job execution, specific credentials are needed to access required resources, for example, BigQuery credential 
for BQ to BQ tasks. Users are able to register secrets on their own, manage it, and use it in tasks and hooks. 
Please go through [concepts](../concepts/overview.md) to know more about it.

## Registering secret with Optimus

Register a secret by running the following command:
```shell
$ optimus secret set someSecret someSecretValue
````

By default, Optimus will encode the secret value. However, to register secret that has been encoded, run the following 
command instead:
```shell
$ optimus secret set someSecret encodedSecretValue --base64
````

There is also a flexibility to register using an existing secret file, instead of providing the secret value in the 
command.
```shell
$ optimus secret set someSecret --file=/path/to/secret
```

Please note that registering a secret which already exists will result in error. Modifying an existing secret can be 
done using the Update command.

## Updating a secret

The update-only flag is generally used when you explicitly only want to update a secret which already exists
and don't want to create it by mistake.

```shell
$ optimus secret set someSecret someSecretValue --update-only
```

It will return an error if the secret to update does not exist already.


## Listing a secret

The list command can be used to show the user defined secrets which are registered with optimus. It will list
the namespace associated for a secret.

```shell
$ optimus secret list
Secrets for project: optimus-local
     NAME    |                    DIGEST                    | NAMESPACE |         DATE
-------------+----------------------------------------------+-----------+-----------------------
   secret1   | SIBzsgUuHnExBY4qSzqcrlrb+3zCAHGu/4Fv1O8eMI8= |     *     | 2022-04-12T04:30:45Z
```

It shows a digest for the encrypted secret, so as not to send the cleartext password on the network.
