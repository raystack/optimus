# Installation

Installing Optimus on any system is straight forward. There are several approaches to install Optimus:

- Using a pre-built binary
- Installing with package manager
- Installing with Docker
- Installing from source

## Using a Pre-built Binary

The client and server binaries are downloadable at the [releases](https://github.com/raystack/optimus/releases) section.

Once installed, you should be able to run:

```shell
$ optimus version
```

## Installing with Package Manager

For macOS, you can install Optimus using homebrew:

```shell
$ brew install raystack/tap/optimus
$ optimus version
```

## Installing using Docker

To pull latest image:

```shell
$ docker pull raystack/optimus:latest
```

To pull specific image:

```shell
$ docker pull raystack/optimus:0.6.0
```

## Installing from Source

### Prerequisites

Optimus requires the following dependencies:

- Golang (version 1.18 or above)
- Git

### Build

Run the following commands to compile optimus from source

```shell
$ git clone git@github.com:raystack/optimus.git
$ cd optimus
$ make build
```

Use the following command to test

```shell
$ optimus version
```
