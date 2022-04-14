---
id: building-plugin
title: Developing plugins
---

Optimus's responsibilities are currently divided in two parts, scheduling a transformation [task](../concepts/overview.md#Job) and running one time action to create or modify a [datastore](../concepts/overview.md#Datastore) resource. Defining how a datastore is managed can be easy and doesn't leave many options for configuration or ambiguity although the way datastores are implemented gives developers flexibility to contribute additional type of datastore, but it is not something we do every day.

Whereas tasks used in jobs that define how the transformation will execute, what configuration does it need as input from user, how does this task resolves dependencies between each other, what kind of assets it might need. These questions are very open and answers to them could be different in  different organisation and users. To allow flexibility of answering these questions by developers themselves, we have chosen to make it easy to  contribute a new kind of task or even a hook. This modularity in Optimus is achieved using plugins.

> Plugins are self-contained binaries which implements predefined protobuf interfaces to extend Optimus functionalities.

Optimus can be divided in two logical parts when we are thinking of a pluggable model, one is the **core** where everything happens which is common for all job/datastore, and the other part which could be variable and needs user specific definitions of how things should work which is a **plugin**.

## Types of Plugins in Optimus
At the moment mainly there are two types of plugins which optimus supports. These are : ***Hook*** and  ***Task***
Before getting into the difference between two plugins ,we need to get familiar with [Jobs](../concepts/overview.md#Job).

| Type                   | Task                                                         | Hook                                                         |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Definition             | It is the single base transformation in a Job.               | It is the operation that we preferably run before or after a Job. |
| Fundamental Difference | It can have dependencies over other jobs inside the Optimus project. | It can have dependencies over other hooks within the job.    |
| Configuration          | It has its own set of configs and asset directory.           | It has its own set of configs and share the same asset directory across all hooks as the base job. |


## Creating a plugin

At the moment Optimus supports task as well as hook plugins. In this section we will be explaining how to write a new task although both are very similar. Plugins are implemented using [go-plugin](https://github.com/hashicorp/go-plugin) developed by Hashicorp used in terraform and other similar products. 

> Plugins can be implemented in any language as long as they can be exported as a single self-contained executable binary and implements a GRPC server. 

It is recommended to use Go currently for writing plugins because of its cross platform build functionality and to reuse protobuf sdk provided within Optimus core. Although the plugin is written in Go, it will be just an adapter between what actually needs to be executed. Actual transformation will be packed in a docker image and Optimus will execute these arbitrary docker images as long as it has access to reach container registry. 

> Plugin binary itself is not executed for transformation but only used for adapting conditions which Optimus requires to be defined for each task.

To demonstrate this wrapping functionality, let's create a plugin in Go and use python for actual transformation logic. You can choose to fork this [example](https://github.com/kushsharma/optimus-plugins) template and modify it as per your needs or start fresh. To demonstrate how to start from scratch, we will be starting from an empty git repository and build a plugin which will find potential hazardous **Near Earth Orbit** objects every day, let's call it **neo** for short. 

Brief description of Neo is as follows

- Using  [NASA API](https://api.nasa.gov/) we can get count of hazardous objects, their diameter and velocity.
- Task will need two config as input, `RANGE_START`, `RANGE_END` as date time string which will filter the count for this specific period only.
- Execute every day, say at 2 AM.
- Need a secret token that will be passed to NASA api endpoint for each request.
- Output of this object count can be printed in logs for now but in a real use case can be pushed to Kafka topic or written to a database.
- Plugin will be written in **Go** and **Neo** in **python**.

### Preparing task executor

Start by initialising an empty git repository with the following folder structure

```shell
.git
/task
  /neo
    /executor
      /main.py
      /requirements.txt
      /Dockerfile
README.md
```

That is three folders one inside another. This might look confusing for now, a lot of things will, but just keep going. Create an empty python file in executor `main.py`, this is where the main logic for interacting with nasa api and generating output will be. For simplicity, lets use as minimal things as possible.

Add the following code to `main.py`

```python
import os
import requests
import json

def start():
    """
    Sends a http call to nasa api, parses response and prints potential hazardous
    objects in near earth orbit
    :return:
    """
    opt_config = fetch_config_from_optimus()

    # user configuration for date range
    range_start = opt_config["envs"]["RANGE_START"]
    range_end = opt_config["envs"]["RANGE_END"]

    secret_key = os.environ["SECRET_KEY"]

    # secret token required for NASA API being passed using job spec
    api_key = json.loads(secret_key)
    if api_key is None:
        raise Exception("invalid api token")

    # send the request for given date range
    r = requests.get(url="https://api.nasa.gov/neo/rest/v1/feed",
                     params={'start_date': range_start, 'end_date': range_end, 'api_key': api_key})

    # extracting data in json format
    print("for date range {} - {}".format(range_start, range_end))
    print_details(r.json())

    return
  

def fetch_config_from_optimus():
    """
    Fetch configuration inputs required to run this task for a single schedule day
    Configurations are fetched using optimus rest api
    :return:
    """
    # try printing os env to see what all we have for debugging
    # print(os.environ)

    # prepare request
    optimus_host = os.environ["OPTIMUS_HOSTNAME"]
    scheduled_at = os.environ["SCHEDULED_AT"]
    project_name = os.environ["PROJECT"]
    job_name = os.environ["JOB_NAME"]

    r = requests.post(url="http://{}/api/v1/project/{}/job/{}/instance".format(optimus_host, project_name, job_name),
                      json={'scheduled_at': scheduled_at,
                            'instance_name': "neo",
                            'instance_type': "TASK"})
    instance = r.json()

    # print(instance)
    return instance["context"]
  
 
  
if __name__ == "__main__":
    start()
```



`api_key` is a token provided by nasa during registration. This token will be passed as a parameter in each http call. `SECRET_PATH` is the path to a file which will contain this token in json and will be mounted inside the docker container by Optimus.

`start` function is using `fetch_config_from_optimus()` to get the date range for which this task executes for an iteration. In this example, configuration is fetched using REST APIs provided by optimus although there are variety of ways to get them. After extracting `API_KEY` from secret file, unmarshalling it to json with ` json.load()` send a http request to nasa api. Response can be parsed and printed using the following function

```python
def print_details(jd):
    """
    Parse and calculate what we need from NASA endpoint response

    :param jd: json data fetched from NASA API
    :return:
    """
    element_count = jd['element_count']
    potentially_hazardous = []
    for search_date in jd['near_earth_objects'].keys():
        for neo in jd['near_earth_objects'][search_date]:
            if neo["is_potentially_hazardous_asteroid"] is True:
                potentially_hazardous.append({
                    "name": neo["name"],
                    "estimated_diameter_km": neo["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                    "relative_velocity_kmh": neo["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]
                })

    print("total tracking: {}\npotential hazardous: {}".format(element_count, len(potentially_hazardous)))
    for haz in potentially_hazardous:
        print("Name: {}\nEstimated Diameter: {} km\nRelative Velocity: {} km/h\n\n".format(
            haz["name"],
            haz["estimated_diameter_km"],
            haz["relative_velocity_kmh"]
        ))
    return
```



Finish it off by adding the main function

```python
if __name__ == "__main__":
    start()
```



Add `requests` library in `requirements.txt`

```ini
requests==v2.25.1
```



Once the python code is ready, wrap this in a `Dockerfile`

```dockerfile
# set base image (host OS)
FROM python:3.8

# set the working directory in the container
RUN mkdir -p /opt
WORKDIR /opt

# copy the content of the local src directory to the working directory
COPY task/neo/executor .

# install dependencies
RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
```



Now that base image is ready for execution, this needs to be scheduled at a fixed interval using `jobs` but for optimus to understand **Neo** task, we need to write an adapter for it.

### Implementing plugin interface

Because we are using Go, start by initialising Go module in `neo` directory as follows

```go
go mod init github.com/kushsharma/optimus-plugins/task/neo
```

Prepare `main.go` in the same directory structure

```
.git
/task
  /neo
    /executor
      /main.py
      /requirements.txt
      /Dockerfile
    /main.go
    /go.mod
    /go.sum
README.md
```



Start by adding the following boilerplate code

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/go-hclog"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin"
	"github.com/odpf/optimus/plugin/base"
)

var (
	Name    = "neo"
	Version = "latest"
	Image   = "ghcr.io/kushsharma/optimus-task-neo-executor"
)

type Neo struct{}

func main() {
	plugin.Serve(func(log hclog.Logger) interface{} {
		return &Neo{
			log: log,
		}
	})
}
```



The plugin binary serves a GRPC server on start but before the communication channel is created protocol version, socket, port, and some other metadata needs to be printed as the handshake information to stdout which the core will read. Plugin and core needs to mutually conclude on same protocol version to start the communication. Client side protocol version announcement is done using `HandshakeConfig` provided in `main()`. 

**Handshake contract:**
CORE-PROTOCOL-VERSION | APP-PROTOCOL-VERSION | NETWORK-TYPE | NETWORK-ADDR | PROTOCOL

**For example:** 

1|1|tcp|127.0.0.1:1234|grpc

You don't have to worry about this if you are using the provided `plugin.Serve` and all this happens automatically behind the scene. Core will initiate a connection with the plugin server as a client when the core binary boots and caches the connection for further internal use.

A single binary can serve more than one kind of plugin, in this example stick with just one. Each plugin is composed of one `base` plugin implementation and some additional optional `mods`.

#### Base Plugin

Base plugin interface needs to be [implemented](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/base.proto) by every plugin. It is responsible for providing plugin metadata to Optimus core.

```protobuf
syntax = "proto3";
package odpf.optimus.plugins;

// Base must be implemented by all plugins
service Base {
    // PluginInfo provides basic details for this plugin
    rpc PluginInfo(PluginInfoRequest) returns (PluginInfoResponse);
}

// PluginType enumerates the type of plugins Optimus supports
enum PluginType {
    PluginType_UNKNOWN = 0;
    PluginType_TASK = 1;
    PluginType_HOOK = 2;
}

// PluginMod enumerates the type of mods this plugin supports
enum PluginMod {
    PluginMod_UNKNOWN = 0;
    PluginMod_CLI = 1;
    PluginMod_DEPENDENCYRESOLVER = 2;
}

// HookType enumerates the type of hook Optimus supports
enum HookType {
    HookType_UNKNOWN = 0;
    HookType_PRE = 1;
    HookType_POST = 2;
    HookType_FAIL = 3;
}

message PluginInfoRequest {}
message PluginInfoResponse {
    string name = 1;
    string description = 2;
    PluginType plugin_type = 3;
    repeated PluginMod plugin_mods = 4;

    // plugin_version is the semver version of this individual plugin
    string plugin_version = 5;
    // api_versions indicates the versions of the Optimus Plugin API
    // this plugin supports
    repeated string api_version = 6;

    // docker image including version if this executes a docker image
    string image = 10;

    // HOOK specific
    // name of hooks on which this should depend on before executing
    repeated string depends_on = 20;
    HookType hook_type = 21;

    // Experimental
    // will be mounted inside the container as volume
    string secret_path = 30;
}
```

If your plugin simply wants to register itself as task or hook for execution and nothing else then that's it. You don't need to implement anything else but for additional features we can implement plugin `mod`.

#### Plugin Mods

Plugin can have none or many plugins mods being implemented at the same time. At the moment there are 2 mods available for usage

1. [CLIMod](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/cli.proto): It provides plugin to interact with Optimus cli. Plugin can provide default configs, ask questions from users to create job specification, override default asset macro compilation behaviour, etc.
2. [DependencyResolverMod](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/dependency_resolver.proto): It provides plugin to implement automatic dependency resolution using assets/configs.

In this example we will use the CLIMod.

To start serving GRPC, either we write our own implementation for serialising/deserialising Go structs to protobufs or reuse the one already provided by [core](https://github.com/odpf/optimus/blob/eaa50bb37d7e738d9b8a94332312f34b04a7e16b/plugin/task/server.go). Optimus GRPC server accepts an interface which we will implement next on Neo struct. Custom protobuf adapter can also be written using the [provided](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/base.proto) protobuf stored in odpf [repository](https://github.com/odpf/proton).

Add the following code in the existing `main.go` as an implementation to [BasePlugin](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/base.proto)

```go
type Neo struct{}

// GetSchema provides basic task details
func (n *Neo) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          Name,
		Description:   "Near earth object tracker",
		PluginType:    models.PluginTypeTask,
		PluginMods:    []models.PluginMod{models.ModTypeCLI},
		PluginVersion: Version,
		APIVersion:    []string{strconv.Itoa(base.ProtocolVersion)},

		// docker image that will be executed as the actual transformation task
		Image: fmt.Sprintf("%s:%s", Image, Version),
	}, nil
}
```

You might have noticed we have specified at line number 9 that we are supporting `models.ModTypeCLI`. This will let Optimus know what all this plugin is capable of. Let's implement the [CLIMod](https://github.com/odpf/proton/blob/54e0bec2df4235cabea4ac2127534a468584e932/odpf/optimus/plugins/cli.proto) now.


```go
// GetQuestions provides questions asked via optimus cli when a new job spec
// is requested to be created
func (n *Neo) GetQuestions(ctx context.Context, req models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	tQues := []models.PluginQuestion{
		{
			Name:   "Start",
			Prompt: "Date range start",
			Help:   "YYYY-MM-DD format",
		},
		{
			Name:   "End",
			Prompt: "Date range end",
			Help:   "YYYY-MM-DD format",
		},
	}
	return &models.GetQuestionsResponse{
		Questions: tQues,
	}, nil
}

// ValidateQuestion validate how stupid user is
// Each question config in GetQuestions will send a validation request
func (n *Neo) ValidateQuestion(ctx context.Context, req models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	var err error
	switch req.Answer.Question.Name {
	case "Start":
		err = func(ans interface{}) error {
			d, ok := ans.(string)
			if !ok || d == "" {
				return errors.New("not a valid string")
			}
			// can choose to parse here for a valid date but we will use optimus
			// macros here {{.DSTART}} instead of actual dates
			// _, err := time.Parse(time.RFC3339, d)
			// return err
			return nil
		}(req.Answer.Value)
	case "End":
		err = func(ans interface{}) error {
			d, ok := ans.(string)
			if !ok || d == "" {
				return errors.New("not a valid string")
			}
			// can choose to parse here for a valid date but we will use optimus
			// macros here {{.DEND}} instead of actual dates
			// _, err := time.Parse(time.RFC3339, d)
			// return err
			return nil
		}(req.Answer.Value)
	}
	if err != nil {
		return &models.ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &models.ValidateQuestionResponse{
		Success: true,
	}, nil
}

func findAnswerByName(name string, answers []models.PluginAnswer) (models.PluginAnswer, bool) {
	for _, ans := range answers {
		if ans.Question.Name == name {
			return ans, true
		}
	}
	return models.PluginAnswer{}, false
}

// DefaultConfig are a set of key value pair which will be embedded in job
// specification. These configs can be requested by the docker container before
// execution
// PluginConfig Value can contain valid go templates and they will be parsed at
// runtime
func (n *Neo) DefaultConfig(ctx context.Context, request models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	start, _ := findAnswerByName("Start", request.Answers)
	end, _ := findAnswerByName("End", request.Answers)

	conf := []models.PluginConfig{
		{
			Name:  "RANGE_START",
			Value: start.Value,
		},
		{
			Name:  "RANGE_END",
			Value: end.Value,
		},
	}
	return &models.DefaultConfigResponse{
		Config: conf,
	}, nil
}

// DefaultAssets are a set of files which will be embedded in job
// specification in assets folder. These configs can be requested by the
// docker container before execution.
func (n *Neo) DefaultAssets(ctx context.Context, _ models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	return &models.DefaultAssetsResponse{}, nil
}

// override the compilation behaviour of assets - if needed
func (n *Neo) CompileAssets(ctx context.Context, req models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	return &models.CompileAssetsResponse{
		Assets: req.Assets,
	}, nil
}
```



All the functions are prefixed with comments to give you basic idea of what each one is doing, for advanced usage, look at other plugins used in the wild.

Few things to note:

- `PluginInfo` is used to define a unique `name` used by your plugin to identify yourself, keep it simple. 
- `PluginInfo` contains `Image` field that specify the docker image which Optimus will execute when needed. This is where the neo python image will go.
- `Version` field can be injected using build system, here we are only keeping a default value.
- `PluginType` in `PluginInfo` will tell of this plugin should be read as `Task` or `Hook` by Optimus core.



### Building everything

Once the code is ready, to build there is a pretty nice tool available for golang [goreleaser](https://github.com/goreleaser/goreleaser/). A single configuration file will contain everything to build the docker image as well as the binary.

Put this in the root of the project as `.goreleaser.yml`

```yaml
builds:
  - dir: ./task/neo
    main: .
    id: "neo"
    binary: "optimus-neo_{{.Os}}_{{.Arch}}"
    ldflags:
      - -s -w -X main.Version={{.Version}} -X main.Image=ghcr.io/kushsharma/optimus-task-neo-executor
    goos:
      - linux
      - darwin
      - windows
    goarch:
      - amd64
      - arm64
    env:
      - CGO_ENABLED=0
archives:
  - replacements:
      darwin: darwin
      linux: linux
      windows: windows
      amd64: amd64
    format_overrides:
      - goos: windows
        format: zip
release:
  prerelease: auto
checksum:
  name_template: 'checksums.txt'
snapshot:
  name_template: "{{ .Tag }}-next"
changelog:
  sort: asc
  filters:
    exclude:
      - '^docs:'
      - '^test:'
dockers:
  -
    goos: linux
    goarch: amd64
    image_templates:
    - "ghcr.io/kushsharma/optimus-task-neo-executor:latest"
    - "ghcr.io/kushsharma/optimus-task-neo-executor:{{ .Version }}"
    dockerfile: ./task/neo/executor/Dockerfile
    extra_files:
    - task/neo/executor

brews:
  - name: optimus-plugins-kush
    install: |
      bin.install Dir["optimus-*"]
    tap:
      owner: kushsharma
      name: homebrew-taps
    license: "Apache 2.0"
    description: "Optimus plugins - [Optimus Near earth orbit tracker]"
    commit_author:
      name: Kush Sharma
      email: 3647166+kushsharma@users.noreply.github.com
```

Please go through goreleaser documentation to understand what this config is doing but just to explain briefly

- It will build golang task plugin adapter for multiple platforms, archives them and release on Github
- Build and push the docker image for the python neo task
- Create a Formula for installing this plugin on Mac using brew
- Each plugin will follow the binary naming convention as `optimus-<pluginname>_<os>_<arch>`. For example: `optimus-bq2bq_linux_amd64`

Once installed, run goreleaser using

```shell
goreleaser --snapshot --rm-dist
```

Use this [repository](https://github.com/kushsharma/optimus-plugins) as an example to see how everything fits in together. It uses github workflows to run goreleaser and publish everything.

## How to use

### Installing a plugin

Plugins need to be installed in Optimus server before it can be used. Optimus uses following directories for discovering plugin binaries

```shell
./
<exec>/
<exec>/.optimus/plugins
$HOME/.optimus/plugins
/usr/bin
/usr/local/bin
```

If Optimus cli is used to generate specifications or deployment, plugin should be installed in a client's machine as well. 

> Plugins can potentially modify the behavior of Optimus in undesired ways. Exercise caution when adding new plugins developed by unrecognized developers.

### Using in job specification

Once everything is built and in place, we can generate job specifications that uses **neo** as the task type.

```shell
optimus create job
? What is the job name? is_last_day_on_earth
? Who is the owner of this job? kush.sharma@example.io
? Which task to run? neo
? Specify the start date 2021-05-25
? Specify the interval (in crontab notation) 0 2 * * *
? Transformation window daily
? Date range start {{.DSTART}}
? Date range end {{.DEND}}
job created successfully is_last_day_on_earth
```

Create a commit and deploy this specification if you are using optimus with a git managed repositories or send a REST call or use GRPC, whatever floats your boat.

### Checking the output

If your optimus deployment is using Airflow as the scheduling engine, open the task log and verify something like this

```
total tracking: 14
potential hazardous: 1
Name: (2014 KP4)
Estimated Diameter: 0.8204270649 km
Relative Velocity: 147052.9914506647 km/h
```

## Additional details

A task is like a pipeline, it takes some input, it runs a procedure on the input and then produces an output. Procedure is wrapped inside the docker image, output is owned by the procedure which could be anything but input should be injected somehow by optimus or at least provide some information about where/what input is. Currently, Optimus supports two kind of inputs:

- Key value configuration
- File assets

##### Task Configuration

Task configurations are key value pair provided as part of job specification in `job.yaml` file. These are based on plugin implementation of `TaskPlugin` interface. These configurations accept simple strings as well as Optimus [macros](../concepts/overview.md#Macros-&-Templates). There are few Optimus provided configuration to all tasks and hooks even if users don't specifically provide them:

- DSTART
- DEND
- EXECUTION_TIME

Secrets can be used in the task configuration by using macros like ` SECRET_PATH: "{{.secret.api_key_path}}" `.
Where `api_key_path` is a value stored inside secrets. This configuration in macros will be replaced by optimus serer while sending a request to the plugin with actual value and can be used by the plugin. In the executor of the plugin, this configuration will be made available as environment variable.

##### File Assets

Sometimes a task may need more than just key value configuration, this is where assets can be used. Assets are packed along with the job specification and should have unique names. A task can have more than one asset file but if any file name conflicts with any already existing plugin in the optimus, it will throw an error, so it is advised to either prefix them or name them very specific to the task. These assets should ideally be small and not more than ~5 MB and any heavy lifting if required should be done directly inside the task container.

### Requesting task context

Optimus calls these task configuration and asset inputs for each scheduled execution of a task as `context`. There are variety of ways to fetch task context from optimus.

- REST API
- GRPC function call
- Optimus cli

##### REST API

This is probably the easiest way using [REST API](https://github.com/odpf/optimus/blob/0ab5a4d44a7b2b85e9a160aef3648d8ba798536a/third_party/OpenAPI/odpf/optimus/runtime_service.swagger.json#L187) provided by optimus server. Each container when boots up has few pre-defined environment variables injected by optimus, few of them are:

- JOB_NAME
- OPTIMUS_HOSTNAME
- JOB_DIR
- PROJECT
- SCHEDULED_AT
- INSTANCE_TYPE
- INSTANCE_NAME

These variables might be needed to make the call and in response, container should get configuration and files as key value pairs in json.

##### GRPC call

Plugin can choose to make a GRPC call using `RegisterInstance` [function](https://github.com/odpf/proton/blob/main/odpf/optimus/runtime_service.proto#L124) and should get the context back in return.

##### Optimus cli

There could be scenarios where it is not possible or maybe not convenient to modify the base execution image and still task need context configuration values. One easy way to do this is by wrapping the base docker image into another docker image and using optimus binary to request task context. Optimus command will internally send a GRPC call and store the output in `${JOB_DIR}/in/` directory. It will create one `.env` file containing all the configurations, one `.secret` file with environment variables with potentially sensitive values, and all the asset files belong to the provided task. Optimus command can be invoked as

```shell
OPTIMUS_ADMIN_ENABLED=1 /opt/optimus admin build instance $JOB_NAME --project $PROJECT --output-dir $JOB_DIR --type $INSTANCE_TYPE --name $INSTANCE_NAME --scheduled-at $SCHEDULED_AT --host $OPTIMUS_HOSTNAME
```

You might have noticed, optimus need OPTIMUS_ADMIN_ENABLED as env variable to enable admin commands. An example of wrapper `Dockerfile` is as follows

```dockerfile
FROM ghcr.io/kushsharma/optimus-task-neo-executor:latest

# path to optimus release tar.gz
ARG OPTIMUS_RELEASE_URL

RUN apt install curl tar -y
RUN mkdir -p /opt
RUN curl -sL ${OPTIMUS_RELEASE_URL} | tar xvz optimus
RUN mv optimus /opt/optimus || true
RUN chmod +x /opt/optimus

# or copy like this
COPY task/neo/example.entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["python3", "/opt/main.py"]
```

Where `entrypoint.sh` is as follows

```shell
#!/bin/sh
# wait for few seconds to prepare scheduler for the run
sleep 5

# get resources
echo "-- initializing optimus assets"
OPTIMUS_ADMIN_ENABLED=1 /opt/optimus admin build instance $JOB_NAME --project $PROJECT --output-dir $JOB_DIR --type $TASK_TYPE --name $TASK_NAME --scheduled-at $SCHEDULED_AT --host $OPTIMUS_HOSTNAME

# TODO: this doesnt support using back quote sign in env vars
echo "-- exporting env"
set -o allexport
source $JOB_DIR/in/.env
set +o allexport

echo "-- current envs"
printenv

echo "-- exporting secret envs"
set -o allexport
source $JOB_DIR/in/.secret
set +o allexport

echo "-- running unit"
exec $(eval echo "$@")
```

All of it can be built using goreleaser as well

```yaml
dockers:
  -
    goos: linux
    goarch: amd64
    image_templates:
    - "ghcr.io/kushsharma/optimus-task-neo:latest"
    - "ghcr.io/kushsharma/optimus-task-neo:{{ .Version }}"
    dockerfile: ./task/neo/example.Dockerfile
    extra_files:
    - task/neo/example.entrypoint.sh
    build_flag_templates:
    - "--build-arg=OPTIMUS_RELEASE_URL=https://github.com/odpf/optimus/releases/download/v0.0.1-rc.2/optimus_0.0.1-rc.2_linux_x86_64.tar.gz"
```

Keep in mind, the plugin binary now needs to point to this `optimus-task-neo` docker image and not the base one. An example of this approach can be checked in the provided [repository](https://github.com/kushsharma/optimus-plugins).

### Directory Structure

You might have already understood it by now but still just to state, the reason we went ahead with the provided directory structure earlier so that we can support more than one task and even hooks if we need to in the same repository. Image a single repository of plugins as an organization repository where one can find all that can be contributed by an entity

```
/task
  /neo
    /executor
      /main.py
      /requirements.txt
      /Dockerfile
    /main.go
    /go.mod
    /go.sum
  /task-2
  /task-3
/hook
  /hook-1
  /hook-2
.goreleaser.yml
README.md
```

### Secret management

You must be wondering from where that api token came from when we said it will be mounted inside the container. Optimus need to somehow know what the secret is, for this current implementation of optimus relies on Kubernetes [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/). Optimus is built to be deployed on kubernetes, although it can work just fine without it as well but might need some tweaking here and there. An example of creating this secret 

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: optimus-task-neo
type: Opaque
data:
  key.json: base64encodedAPIkeygoes
```

Notice the name of the secret `optimus-task-neo` which is actually based on a convention. That is if secret is defined, Optimus will look in kubernetes using `optimus-task-<taskname>` as the secret name and mount it to the path provided in `SecretPath` field of `PluginInfo`.

There is also a new way to using secrets in the job for a plugin or otherwise, we can store a user defined secret in the optimus server with the following command
```shell
optimus secret set secret_name <secret_value>
```
Verify if the secret is registered properly with optimus server using following command
```shell
optimus secret list
```
It should list the secret registered above.

Then we can mention the secret in the job spec configuration like mentioned in the Task Configuration section. The configuration with secrets will be made available to the calls to plugins on dependency mod, and set as environment variable in the executor used by the scheduler.

