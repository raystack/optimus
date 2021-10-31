- Feature Name: Secret Management
- Status: draft
- Start Date: 2021-10-02
- Authors: Kush Sharma

# Summary

A lot of transformation operations require credentials to execute, there is a need to have a convenient way to save secrets and then access them in containers during the execution. This secret may also be needed in plugin adapters to compute dependencies/compile assets/etc before the actual transformation even begin. This is currently done using registering a secret to optimus so that it can be accessed by plugins and Kubernetes opaque secret, a single secret per plugin, getting mounted in the container(i.e. not at individual job level).

This can be solved by allowing users to register secret from Optimus CLI as a key value pair, storing them by encrypting using a multi party encryption. There will be two party by default, Optimus server and the Optimus project. All clients will be treated within a project as a single party by default and will be able to access all registered secrets in same project.

# Technical Design

To keep string literals as secret, it is a requirement Optimus keep them encrypted in database. To decrypt them, there will be two keys, one passed to Optimus server as a config value during startup and the other which clients will use to fetch registered secrets called `project key`. Project key will itself be also a secret stored like any other secret in optimus server encrypted using Optimus server key. Each secret is a key value pair where key is an alpha numeric literal and value is base64 encoded string.

Project key will be initialised as part of project registration or project config update if it is not registered already(this is required to make project key creation backward compatible). 

The thing that will differentiate an ordinary project secret and this specific key is the prefix. Each of system level secret should be prefixed by `_OPTIMUS_<key name>`. Optimus should also disallow anyone using this prefix to register their secrets. So when the project gets registered and Optimus server generates the project key, it will be encrypted by Optimus key and stored with the name `_OPTIMUS_<project_name>_ROOT`. It will be used to encrypt all future secrets registered by users under this project.

Reason to keep project key in db is to allow users to create new secrets without requiring them to specify the project key each time.

### Project Config

Optional prerequisite to registering Secrets is configuring the Kubernetes cluster details where the containers will run. This will allow Optimus to create project key in the cluster namespace so that all the containers can use this key to decrypt, once the secrets are registered, as plain text. Although this is not a hard requirement and can be done by cluster admin manually as well. This is a one time operation per registered project.

Sample `.optimus.yaml`

```yaml
version: 1
host: localhost:9100
job:
  path: jobs
config:
  global:
    environment: integration
    kubernetes_host: x.x.x.x
    kubernetes_namespace: optimus

```

### Optimus CLI

User interaction to manage a secret will start from CLI. Users can create/update/list/delete a secret as follows

#### Create

`optimus secret create <name> <value> ` will take a secret name and a base64 encoded string as a pair. It will return the project key if this secret is the first secret registered within the project.

> One thing yet to be decided is, should we scope the secrets at namespace level or project level? (I think at project level)

`optimus secret create <somename> --file="path"` should read the base64 encoded file as value. Additional flag `--base64` should support taking raw value and encode them for user.

> Should we do the other way around? Take raw value by default and use a flag to accept base64 encoded value?

#### Delete

`optimus secret delete` to delete the secret

#### Update

`optimus secret update` to update the secret value

#### List

`optimus secret list` to list all created secrets in a project. List operation will print a digest of the secret and the time at which it was last updated. Digest should be a SHA hash of the encrypted string to simply visualize it as a signature when a secret is changed or the key gets rotated. Example:

```
     NAME     |              DIGEST              |  DATE
  SECRET_1    | 6c463e806738046ff3c78a08d8bd2b70 | 2021-10-06 02:02:02
  SECRET_2    | 3aa788a21a76651c349ceeee76f1cb76 | 2021-10-06 06:02:02
```

#### Using secrets

Secrets can be used as part of the job spec config using macros with their names. This will work as aliasing the secret to be used in containers.

```yaml
task: foo
config:
  do: this
  dsn: {{ .secret.postgres_dsn }}
```

One thing to note is currently we print all the container environment variables using `printenv` command as debug. This should be removed after this RFC is merged to avoid exposing secrets in container logs. Job specification compilation will happen with all project secrets using the key passed by client when requesting the secret. This will allow everyone to request any secret but they can only get the original plain text string if they pass the correct key in the request.

#### Accessing secrets

Secrets gets stored in db using a multi-party encryption. There will be two keys to get back the original plain text. One key that gets passed as env var when Optimus server gets initialized(currently we don't support rotating it.) Other key will be passed to containers as a PSK(pre shared key) which they will use to fetch the same secret as plain text. 

- PSK in case of kubernetes will be created as k8s secret by Optimus if kubernetes cluster details are registered in namespace config.

- PSK in case we deploy these in vms will be manually saved inside the machine(although we don't support deploying optimus for vm transformations)

During a `RegisterInstance` call, project key needs to be supplied as one of the field which along with job config/assets also compiles the secrets requested. Client container will get this key using a k8s secret mounted inside it as env variable. Key secret in k8s will have a fixed name `optimus-<project>-key` inside the deployment namespace. 

If a client tries to extract these secrets as part of job assets/configs without passing a correct party key, job config template will still be compiled but with incorrect key which will generate undefined string as the value.

> All jobs will have access to all the secrets registered for a project.

Because Optimus is deployed in trusted network, we don't need TLS for now to fetch job secrets from the container but once Optimus is deployed as a service on edge network, this communication should only happen over TLS. 

### Using secrets without Optimus

If someone wants to pass an exclusive secret without registering it with Optimus first, that should also be possible. 

- In case of k8s: this can be done using a new field introduced in Job spec as `metadata` which will allow users to mount arbitrary secrets inside the container available in the same k8s namespace.

### Rotating project key

It is not very uncommon for users to expose their key, because all secrets share the same private key, it should be possible to rotate it when needed. For now we will support rotating project key only.

`optimus secret rotate <old_key> [<new_key>]`

`old_key` is the previous project key, it could actually be Optimus key as well(when used by admins) because project secrets can be decrypted via it. It is optional to pass `new_key` and if in case it is omitted, Optimus will generate and return the new key. If k8s config is registered, it should update the k8s deployment namespace opaque secret as well.

This step is internally loading all the secrets that belong to a project to memory, decrypting it with the old_key, and if passed, encrypting it with the new key(else generate it by itself). Rotation of secrets should be done in a single db transaction.

### Drawbacks

- Secrets although can be registered as many as user want, all of them will be encrypted using a single key by default. That means users can access all of them if needed within a project.
- If Optimus key is exposed, all project secrets will be exposed.
- This design will be a breaking change compare to what we support currently and will require all of the secrets to be registered again.

# Unresolved questions

- Secret should be created at project level or namespace level?
- Should we use a symmetric key as a project key, keep it in Optimus database or a public/private key and private key is only ever known to containers/users? If we do decide to use a private/public key pair, only additional advantage we will have is we will not be storing project's private key because public key should be enough to encrypt all future secrets.

# Footnotes

- Multi party encryption via [age](https://github.com/FiloSottile/age)