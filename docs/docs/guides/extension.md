---
id: extension
title: Work with Extension
---

Extension helps the user to include third-party or arbitrary implementation
as part of Optimus. Currently, extension is designed for when the user
is running it as CLI.

## Warning

Extension is basically an executable file outside Optimus. **We do not guarantee whether an extension is safe or not**. We suggest to check the extension itself, whether it is safe to run in your local or not, before installing and running it.

## Limitation

Extension is designed to be similar to [Github extension](https://cli.github.com/manual/gh_extension).
However, since it's still in early stage, some limitations are there.

* extension is only an executable file
* installation only looks at the Github asset according to the running system OS and Architecture
* convention for extension:
  * extension repository should follow `optimus-extension-[name of extension]` (example: [optimus-extension-valor](https://github.com/gojek/optimus-extension-valor))
  * asset being consdered is binary with suffix `...[OS]-[ARC]` (example: when installing [`valor`](https://github.com/gojek/optimus-extension-valor), if the user's OS is Linux and the architecture is AMD64, then installation will consider `valor_linux-amd64` as binary to be executed)

## Creating

Extension is designed to be open. Anyone could create their own extension. And as long as it is avilable, anyone could install it. In order to create it, the following is the basic steps to do:

1. Decide the name of the extension, example: `valor`
2. Create a Github repository that follows the convention, example: `optimus-extension-valor`
3. Put some implementation and asset with name based on the convention, example: `valor_linux-amd64`, `valor_darwin-amd64`, and more.
4. Ensure it is available for anyone to download

## Commands

Optimus support some commands to help operating on extension.

### Installation

The user can run installation using Optimus sub-command `install` under `extension`.
In order to install extension, run the following command:

```zsh
optimus extension install REMOTE [flags]
```

The user can use `--alias` flag to change the command name, since by default, Optimus
will try to figure it out by itself. Although, during this process, sometime
an extension name conflict with the reserved commands. This flag helps to resolve that.
But, do note that this flag cannot be used to rename an **installed** extension.
To do such a thing, check [rename](#rename).

REMOTE is the Github remote path where to look for the extension.
REMOTE can be in the form of:

* OWNER/PROJECT
* github.com/OWNER/PROJECT
* https://www.github.com/OWNER/PROJECT

One example of such extension is [`Valor`](https://github.com/gojek/optimus-extension-valor).
So, going back to the example above, installing it is like this:

```zsh
optimus extension install gojek/optimus-extension-valor
```

or

```zsh
optimus extension install github.com/gojek/optimus-extension-valor
```

or

```zsh
optimus extension install https://github.com/gojek/optimus-extension-valor
```

Installation process is then in progress. If installation is a success, the user can show it by running:

```zsh
optimus --help
```

A new command named after the extension will be available.
For example, if the extension name is `optimus-extension-valor`, then by default the command named `valor` will be available.
If the user wish to change it, they can use `--alias` during installation, or
`rename` it (explained later).

The following is example when running `optimus` (without any command):

```zsh
...
Available Commands:
  ...
  extension   Operate with extension
  ...
  valor       Execute gojek/optimus-extension-valor [v0.0.2] extension
  version     Print the client version information
...
```

### Executing

In order to execute an extension, make sure to follow the installation process described [above](#installation).
After installation is finished, simply run the extension with the following command:

```zsh
optimus [extension name or alias]
```

Example of `valor`:

```zsh
optimus valor
```

### Operation

The user can do some operations to an extension. This section explain more about the available commands. Do note that these commands are available on the installed extensions.
For more detail, run the following command:

```zsh
optimus extension [extension name or alias]
```

Example:

```zsh
optimus extension valor
```

The above command shows all available commands for `valor` extension.

Output:

```zsh
Sub-command to operate over extension [gojek/optimus-extension-valor@v0.0.4]

USAGE
  optimus extension valor [flags]

CORE COMMANDS
  activate    activate is a sub command to allow user to activate an installed tag
  rename      rename is a sub command to allow user to rename an extension command
  uninstall   uninstall is a sub command to allow user to uninstall a specified tag of an extension
  upgrade     upgrade is a sub command to allow user to upgrade an extension command

INHERITED FLAGS
      --help       Show help for command
      --no-color   Disable colored output
  -v, --verbose    if true, then more message will be provided if error encountered
```

#### Activate

Activate a specific tag when running extension. For example, if the user has two version of `valor`, which is `v0.0.1` and `v0.0.2`, then by specifying the correct tag, the user can just switch between tag.

Example:

```zsh
optimus extension valor activate v0.0.1
```

#### Rename

Rename a specific extension to another command that are not reserved.
By default, Optimus tries to figure out the appropriate command name from its project name.
However, sometime the extension name is not convenient like it being too long or the user
just want to change it.

Example:

```zsh
optimus extension valor rename vl
```

#### Uninstall

Uninstalls extension as a whole or only a specific tag. This allows the user to do
some clean up to preserve some storage or to resolve some issues.
By default, Optimus will uninstall the extension as a whole. To target a specific tag,
use flag `--tag`.

Example:

```zsh
optimus extension valor uninstall
```

#### Upgrade

Upgrade allows the user to upgrade a certain extension to its latest tag.
Although the user can use the install command, but using this command is shorter
and easier as the user only needs to specify the installed extension.

Example:

```zsh
optimus extension valor upgrade
```
