- Feature Name: Secret Management
- Status: Draft
- Start Date: 2021-10-02
- Authors: Kush Sharma & Sravan 

# Summary

A lot of transformation operations require credentials to execute, there is a need to have a convenient way to save secrets and then access them in containers during the execution. This secret may also be needed in plugin adapters to compute dependencies/compile assets/etc before the actual transformation even begin. This is currently done using registering a secret to optimus so that it can be accessed by plugins and Kubernetes opaque secret, a single secret per plugin, getting mounted in the container(i.e. not at individual job level).

This can be solved by allowing users to register secret from Optimus CLI as a key value pair, storing them.

# Technical Design

To keep string literals as secret, it is a requirement Optimus keep them encrypted in database. Optimus Server Key is used to encrypt & decrypt the secret & will ensure the secret is encrypted at rest. Each secret is a key value pair where key is an alpha numeric literal and value is base64 encoded string. 

Optimus has two sets of secrets, user managed secrets & others which are needed for server operations, that will differentiate an ordinary project secret and this specific key is the prefix. Each of server managed secrets should be prefixed by `_OPTIMUS_<key name>` and will not be allowed to be used by users. Optimus should also disallow anyone using this prefix to register their secrets. The secrets can be namespaced by optimus namespace or at project level, which will help in proper authorization & access restrictions.

#### Using secrets

Secrets can be used as part of the job spec config using macros with their names. This will work as aliasing the secret to be used in containers. Only the secrets created at project & namespace the job belongs to can be referenced. So, for the plugin writers any secret that plugin needs can be accessed through environment variables defined in the job spec or can get the secrets by defining in any assets.

```yaml
task: foo
config:
  do: this
  dsn: {{ .secret.postgres_dsn }}
```

One thing to note is currently we print all the container environment variables using `printenv` command as debug. This should be removed after this RFC is merged to avoid exposing secrets in container logs.

Only the admins & containers to be authorized for this end point, as this will allow access to all secrets.

Because Optimus is deployed in trusted network, we don't need TLS for now to fetch job secrets but once Optimus is deployed as a service on edge network, this communication should only happen over TLS. 

### Optimus CLI

User interaction to manage a secret will start from CLI. Users can create/update/list/delete a secret as follows

By default secrets will be created under their namespace, but optionally the secret can be created at project level. We expect proper authorization is set up to handle misuse.

Secrets can be accessed by providing the project & namespace the secret is created in, if the secret is created at project level then namespace can be set to empty string if optimus.yaml already has the namespace configured.

#### Create/Update

`optimus secret create/update <name> <value> ` will take a secret name and value

`optimus secret create/update <name> --file="path"` should read the file content as value. 

Additional flag `--base64` can  be provided by user stating the value is already encoded, if not provided optimus ensures to encode & store it, basic checks can be done to check if the string is a valid base64 encoded string.

#### Delete

`optimus secret delete <name>` 

#### List

`optimus secret list` to list all created secrets in a project/namespace, along with the creation/updated time, will be helpful such that users can use in the job spec, as users might forget the key name, this will not list the system managed secrets.

List operation will print a digest of the secret. Digest should be a SHA hash of the encrypted string to simply visualize it as a signature when a secret is changed or the key gets rotated & the actual secret values will not be allowed to read by end users, as they already have it.

 Example:

```
     NAME     |              DIGEST              |  DATE
  SECRET_1    | 6c463e806738046ff3c78a08d8bd2b70 | 2021-10-06 02:02:02
  SECRET_2    | 3aa788a21a76651c349ceeee76f1cb76 | 2021-10-06 06:02:02
```

### Using secrets without Optimus

If someone wants to pass an exclusive secret without registering it with Optimus first, that should also be possible. 

- In case of k8s: this can be done using a new field introduced in Job spec as `metadata` which will allow users to mount arbitrary secrets inside the container available in the same k8s namespace.

### Rotating Optimus Server key

Secrets need to be rotated whenever we feel the secrets are compromised, user secrets will be rotated by user & the communication can be made to all users when server key is compromised. 

As the server key is configured through environment variable, the rotation can happen by configuring through environment variables. There can be two environment variables for server keys `OLD_APP_KEY` & `APP_KEY`. During startup sha of the `OLD_APP_KEY` is compared with the sha stored in the database, if it matches then rotation will happen and at the end of rotation the sha will be replaced with  `APP_KEY's` sha. The comparision is needed to check to handle the situation of restarts. This rotation will be done only in the master node.

This step is internally loading all the secrets that belong to a project to memory, decrypting it with the old_key, and encrypting it with the new key. Rotation of secrets should be done in a single db transaction.

#### Migration

- This design will be a breaking change compare to how the secrets are handled and will require all of the current secrets to be registered again.

# Footnotes & References

- Multi party encryption via [age](https://github.com/FiloSottile/age)
- [Key Management Services ](https://gocloud.dev/howto/secrets/)

