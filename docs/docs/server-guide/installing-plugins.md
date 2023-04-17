# Installing Plugins
Plugin needs to be installed in the Optimus server before it can be used. Optimus uses the following directories for 
discovering plugin binaries

```
./.plugins
./
<exec>/
<exec>/.optimus/plugins
$HOME/.optimus/plugins
/usr/bin
/usr/local/bin
```

Even though the above list of directories is involved in plugin discovery, it is advised to use .plugins in the 
current working directory of the project or Optimus binary.

To simplify installation, you can add plugin artifacts in the server config:
```yaml
plugin:
  artifacts:
   - https://...path/to/optimus-plugin-neo.yaml  # http
   - http://.../plugins.zip # zip
   - ../transformers/optimus-bq2bq_darwin_arm64 # relative paths
   - ../transformers/optimus-plugin-neo.yaml
```

Run below command to auto-install the plugins in the `.plugins` directory.
```shell
$ optimus plugin install -c config.yaml  # This will install plugins in the `.plugins` folder.
```
