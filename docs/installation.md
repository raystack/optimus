# Installation

Installing Optimus on any system is straight forward. 
We provide pre-built [binaries](https://github.com/odpf/optimus/releases), 
Docker Images and support package managers.

## macOS
You can install Optimus using homebrew on macOS:

```shell
brew install odpf/taps/optimus
opctl version
```

## Download Binaries
The client and server binaries are downloadable at the releases tab. There is 
currently no installer available. You have to add the Optimus binary to the PATH 
environment variable yourself or put the binary in a location that is already 
in your $PATH (e.g. /usr/local/bin, ...).

Once installed, you should be able to run:
```shell
opctl version
```
