# Work with Extension
Extension helps you to include third-party or arbitrary implementation as part of Optimus. Currently, the extension is
designed for when the user is running it as CLI.

## Warning
Extension is basically an executable file outside Optimus. *We do not guarantee whether an extension is safe or not*. 
We suggest checking the extension itself, whether it is safe to run in your local or not, before installing and running it.

## Limitation
Extension is designed to be similar to [GitHub extension](https://cli.github.com/manual/gh_extension). However, since 
it's still in the early stage, some limitations are there.
- extension is only an executable file
- installation only looks at the GitHub asset according to the running system OS and Architecture
- convention for extension:
  - extension repository should follow optimus-extension-[name of extension] (example: [optimus-extension-valor](https://github.com/gojek/optimus-extension-valor))
  - asset being considered is binary with suffix ...[OS]-[ARC] (example: when installing valor, if the user's OS is 
  - Linux and the architecture is AMD64, then installation will consider valor_linux-amd64 as binary to be executed)

## Creating
Extension is designed to be open. Anyone could create their own extension. And as long as it is available, 
anyone could install it. In order to create it, the following are the basic steps to do:

- Decide the name of the extension, example: *valor*
- Create a GitHub repository that follows the convention, example: *optimus-extension-valor*
- Put some implementation and asset with the name based on the convention, example: *valor_linux-amd64, valor_darwin-amd64*, and more.
- Ensure it is available for anyone to download

## Commands
Optimus supports some commands to help operate on extension.

### Installation
You can run the installation using Optimus sub-command install under the extension. In order to install an extension, run the following command:
```shell
$ optimus extension install REMOTE [flags]
```

You can use the *--alias* flag to change the command name, since by default, Optimus will try to figure it out by itself. 
Although, during this process, sometimes an extension name conflicts with the reserved commands. This flag helps to 
resolve that. But, do note that this flag cannot be used to rename an *installed* extension. To do such a thing, check rename.

REMOTE is the Github remote path where to look for the extension. REMOTE can be in the form of
- OWNER/PROJECT
- github.com/OWNER/PROJECT
- https://www.github.com/OWNER/PROJECT

One example of such an extension is Valor. So, going back to the example above, installing it is like this:
```shell
$ optimus extension install gojek/optimus-extension-valor@v0.0.4
```
or
```shell
$ optimus extension install github.com/gojek/optimus-extension-valor@v0.0.4
```
or
```shell
$ optimus extension install https://github.com/gojek/optimus-extension-valor@v0.0.4
```

Installation process is then in progress. If the installation is a success, the user can show it by running:
```shell
$ optimus --help
```


A new command named after the extension will be available. For example, if the extension name is *optimus-extension-valor*, 
then by default the command named valor will be available. If the user wishes to change it, they can use *--alias* 
during installation, or *rename* it (explained later).

The following is an example when running Optimus (without any command):

```shell
...
Available Commands:
  ...
  extension   Operate with extension
  ...
  valor       Execute gojek/optimus-extension-valor [v0.0.4] extension
  version     Print the client version information
...
```


### Executing
In order to execute an extension, make sure to follow the installation process described above. After installation 
is finished, simply run the extension with the following command:

```shell
$ optimus [extension name or alias]
```


Example of valor:
```shell
$ optimus valor
```

Operation
The user can do some operations to an extension. This section explains more about the available commands. 
Do note that these commands are available on the installed extensions. For more detail, run the following command:

```shell
$ optimus extension [extension name or alias]
```

Example:
```shell
$ optimus extension valor

The above command shows all available commands for valor extension.

Output:

Sub-command to operate over extension [gojek/optimus-extension-valor@v0.0.4]

USAGE
  optimus extension valor [flags]

CORE COMMANDS
  activate    activate is a sub command to allow user to activate an installed tag
  describe    describe is a sub command to allow user to describe extension
  rename      rename is a sub command to allow user to rename an extension command
  uninstall   uninstall is a sub command to allow user to uninstall a specified tag of an extension
  upgrade     upgrade is a sub command to allow user to upgrade an extension command

INHERITED FLAGS
      --help       Show help for command
      --no-color   Disable colored output
  -v, --verbose    if true, then more message will be provided if error encountered
```

### Activate
Activate a specific tag when running extension. For example, if the user has two versions of valor, 
which is v0.0.1 and v0.0.2, then by specifying the correct tag, the user can just switch between tag.

Example:
```shell
$ optimus extension valor activate v0.0.1
```

### Describe
Describes general information about an extension, such information includes all available releases of an extension 
in the local, which release is active, and more.

Example:
```shell
$ optimus extension valor describe
```


### Rename
Rename a specific extension to another command that is not reserved. By default, Optimus tries to figure out the 
appropriate command name from its project name. However, sometimes the extension name is not convenient like it 
being too long or the user just wants to change it.

Example:
```shell
$ optimus extension valor rename vl
```

### Uninstall
Uninstalls extension as a whole or only a specific tag. This allows the user to do some cleanup to preserve some 
storage or to resolve some issues. By default, Optimus will uninstall the extension as a whole. To target a specific tag, 
use the flag --tag.

Example:
```shell
$ optimus extension valor uninstall
```

### Upgrade
Upgrade allows the user to upgrade a certain extension to its latest tag. Although the user can use the install command, 
using this command is shorter and easier as the user only needs to specify the installed extension.

Example:
```shell
$ optimus extension valor upgrade
```

