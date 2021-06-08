# Installation

Installing Optimus on any system is straight forward. We provide pre-built [binaries](https://github.com/odpf/optimus/releases), 
Docker Images and support package managers.

## MacOS
You can install Optimus using homebrew on macOS:

```shell
brew install odpf/taps/optimus
optimus version
```

## Download Binaries
The client and server binaries are downloadable at the releases tab. There is 
currently no installer available. You have to add the Optimus binary to the PATH 
environment variable yourself or put the binary in a location that is already 
in your $PATH (e.g. /usr/local/bin, ...).

Once installed, you should be able to run:
```shell
optimus version
```

## Compiling from source

### Prerequisites

Optimus requires the following dependencies:
* Golang (version 1.16 or above)
* Git

### Build

Run the following commands to compile `optimus` from source
```shell
git clone git@github.com:odpf/optimus.git
cd optimus
make build
```

Use the following command to test
```shell
./optimus version
```

Optimus service can be started with the following command although there are few required 
[configurations](./reference/configuration.md) for it to start.
```shell
./optimus serve
```