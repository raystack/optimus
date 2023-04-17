# Tutorial of Plugin Development
To demonstrate the previous-mentioned wrapping functionality, let's create a plugin in Go as well as a yaml definition 
and use python for actual transformation logic. You can choose to fork this [example](https://github.com/kushsharma/optimus-plugins) 
template and modify it as per your needs or start fresh. To demonstrate how to start from scratch, we will be starting 
from an empty git repository and build a plugin which will find potential hazardous **Near Earth Orbit** objects every 
day, let's call it **neo** for short.

Brief description of Neo is as follows
- Using NASA [API](https://api.nasa.gov/) we can get count of hazardous objects, their diameter and velocity.
- Task will need two config as input, RANGE_START, RANGE_END as date time string which will filter the count for 
  this specific period only.
- Execute every day, say at 2 AM.
- Need a secret token that will be passed to NASA api endpoint for each request.
- Output of this object count can be printed in logs for now but in a real use case can be pushed to Kafka topic or 
  written to a database.
- Plugin will be written in **YAML** format and Neo in **python**.

## Preparing task executor
Start by initialising an empty git repository with the following folder structure
```
.git
/task
  /neo
    /executor
      /main.py
      /requirements.txt
      /Dockerfile
README.md
```

That is three folders one inside another. This might look confusing for now, a lot of things will, but just keep going. 
Create an empty python file in executor main.py, this is where the main logic for interacting with nasa api and 
generating output will be. For simplicity, lets use as minimal things as possible.

Add the following code to main.py
         
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

**api_key** is a token provided by nasa during registration. This token will be passed as a parameter in each http call. 
**SECRET_PATH** is the path to a file which will contain this token in json and will be mounted inside the docker 
container by Optimus.

**start** function is using **fetch_config_from_optimus()** to get the date range for which this task executes for 
an iteration. In this example, configuration is fetched using REST APIs provided by optimus although there are variety 
of ways to get them. After extracting **API_KEY** from secret file, unmarshalling it to json with **json.load()** 
send a http request to nasa api. Response can be parsed and printed using the following function:

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

Add requests library in requirements.txt
```
requests==v2.25.1
```

Once the python code is ready, wrap this in a Dockerfile

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


## Creating a yaml plugin
The Yaml implementation is as simple as providing the plugin details as below.

```yaml
name: Neo
description: Near earth object tracker
plugintype: task
pluginversion: latest
image: ghcr.io/kushsharma/optimus-task-neo-executor
secretpath: /tmp/.secrets

questions:
- name: RANGE_START
  prompt: Date range start
  help: YYYY-MM-DD format
  required: true
- name: RANGE_END
  prompt: Date range end
  help: YYYY-MM-DD format
  required: true
```

Based on the usecase, additional validation can be added to the questions section.

This yaml plugin can be placed anywhere, however for this tutorial let’s place it under `../task/neo`  directory and 
name it as `optimus-plugin-neo.yaml`.

Note: As part of this tutorial, we are not providing binary plugin tutorial as it is going to be deprecated. 

## How to Use

Before using, let’s install this new plugin in server and client.

### Installing the plugin in server
To use the created plugin in your server, you can simpy add the plugin path in the server config:

```yaml
plugin:
  artifacts:
   - ../task/neo/optimus-plugin-neo.yaml
```

To apply the change, you can follow either of these options:
- Start Optimus server using `--install-plugins` flag, or
- Install the plugin separately before starting the server using `optimus plugin install` command.

_Note: Take a look at installing plugins in server [guide](../server-guide/installing-plugins.md) for more information._

### Installing the plugin in client
Install the plugin in client side by syncing it from server using below command.
```shell
$ optimus plugin sync
````

Once finished, the `Neo` plugin will be available in the `.plugins` directory.

### Use the plugin in job creation

Once everything is built and in place, we can generate job specifications that uses neo as the task type.

```shell
optimus create job
? What is the job name? is_last_day_on_earth
? Who is the owner of this job? owner@example.io
? Which task to run? neo
? Specify the start date 2022-01-25
? Specify the interval (in crontab notation) 0 2 * * *
? Transformation window daily
? Date range start {{.DSTART}}
? Date range end {{.DEND}}
job created successfully is_last_day_on_earth
```

Create a commit and deploy this specification if you are using optimus with a git managed repositories or send 
a REST call or use GRPC, whatever floats your boat.

Once the job has been deployed and run, open the task log and verify something like this
```
total tracking: 14
potential hazardous: 1
Name: (2014 KP4)
Estimated Diameter: 0.8204270649 km
Relative Velocity: 147052.9914506647 km/h
```
