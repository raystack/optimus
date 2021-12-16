---
id: extension
title: Work with Extension
---

Extension helps the user to include third-party or arbitrary implementation
as part of Optimus. Currently, extension is designed for when the user
is running it as CLI.

### Warning

Extension is basically an executable file outside Optimus. We do not guarantee whether an extension is safe or not. We suggest to check the extension itself, whether it is safe to run in your local or not, before installing and running it.

### Limitation

Extension is designed to be similar to [Github extension](https://cli.github.com/manual/gh_extension).
However, since it's still in early stage, some limitations are there.

* currently it is limited to extension stored in Github
* extension is only an executable file
* currently, only [`install`](#installation) command is available
* installation only looks at the Github asset according to the running system OS and Architecture
* if upgrade is required, then the user needs to manually delete the old extension located under `$HOME/.optimus/extensions`
* convention for extension:
  * extension repository should follow `optimus-extension-[name of extension]` (example: [optimus-extension-valor](https://github.com/gojek/optimus-extension-valor))
  * asset being consdered is binary with suffix `...[OS]-[ARC]` (example: when installing [`valor`](https://github.com/gojek/optimus-extension-valor), if the user's OS is Linux and the architecture is AMD64, then installation will consider `valor_linux-amd64` as binary to be executed)

### Creating

Extension is designed to be open. Anyone could create their own extension. And as long as it is avilable, anyone could install it. In order to create it, the following is the basic steps to do:

1. Decide the name of the extension, example: `valor`
2. Create a Github repository that follows the convention, example: `optimus-extension-valor`
3. Put some implementation and asset with name based on the convention, example: `valor_linux-amd64`, `valor_darwin-amd64`, and more.
4. Ensure it is available for anyone to download

### Installation

In order to install extension, run the following command:

```zsh
optimus extension install OWNER/REPO [flags]
```

OWNER is the Github owner and REPO is the repository name.
In the example [`Valor`](https://github.com/gojek/optimus-extension-valor),
the OWNER is `gojek` and the REPO is `optimus-extension-valor`.
So, going back to the example above, it will be like the following:

```zsh
optimus extension install gojek/optimus-extension-valor
```

Installation process is then in progress. If installation is a success, the user can show it by running:

```zsh
optimus --help
```

A new command named after the extension will be available. For example, if the extension name is `optimus-extension-valor`, then the command named `valor` will be available. Example:

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
optimus [name of extension]
```

Example of `valor`:

```zsh
optimus valor
```
