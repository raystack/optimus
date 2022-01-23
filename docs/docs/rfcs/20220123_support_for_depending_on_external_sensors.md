- Feature Name: Support For Depndening on External Sources
- Status: Draft
- Start Date: 2022-01-23
- Authors: 

# Summary

Optimus supports job dependencies, but there is a need for optimus jobs to depend on external sources which are not managed by the optimus server. For example, depending the BQ or GCS data availability or data being managed by another optimus server. Whatever data sources optimus is managing lets have sensors for basic data availability check, in GCS checking for file exists & in BQ taking a select query & returning success when rowcount > 0. For other requirements let's have a http sensor.

# Technical Design

Optimus can add support for all the sensors as libraries, which will be evaulated within the execution envrionment of the user, all variables will be returned for a given scheduled date through the api call which will be used by the actual sensor execution. 

Optimus provides libraries needed for the above operations which can be used in the respective execution environment of the scheduler, currently the library will be offered in python.

The `/intance` api call can accept params to filter what to return to just reduce the unnecessary payload & return only the needed variables, as sensors execute a lot.

#### **Http Sensor**

If the call returns 200 then the sensor succeeds

```yaml
dependencies : 
 type : http
 endpoint : url
 headers :
 body :
  
```



#### BQ Sensor

If the query results in rows then the sensor succeeds

```yaml
dependencies : 
 type : bq
 query : 
 service_account :
  
```



### GCS Sensor

If the path exists then the sensor succeeds

```yaml
dependencies : 
 type : gcs
 path : 
 service_account :  
```

