---
id: create-bigquery-table
title: Create bigquery table
---

A BigQuery table contains individual records organized in rows. Each record is 
composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, 
and other information. You can specify the schema of a table when it is created.
At the moment only native table is supported.

There are 3 ways to create a table:

### Creating table with Optimus

Supported datastore can be selected by calling
```bash
optimus create resource
```
Optimus will request a resource name which should be unique across whole datastore.
All resource specification contains a name field which conforms to a fixed format.
In case of bigquery table, format should be
`projectname.datasetname.tablename`.
After the name is provided, `optimus` will create a file in configured datastore 
directory. Open the created specification file and add additional spec details
as follows:
```yaml
version: 1

# unique name that must conform to validations of type of resource we are creating
# in case of bigquery table, this is fully qualified name
name: temporary-project.optimus-playground.first_table

# type of resource that belong to this datastore
# e.g.: table, dataset, view
type: table

# labels being passed to datastore which will be injected in the bigquery table
labels:
  usage: testtable
  owner: optimus

# actual specification details that matches the type we are trying to create/update
spec:
  description: "example description"
  schema:
    - name: colume1 # name of the column
      type: INTEGER # datatype of the column
    - name: colume2
      type: TIMESTAMP
      description: "example field 2" # description for the column
      mode: required # possible options (repeated/required/nullable), default is nullable
    - name: colume3
      type: STRUCT
      schema: # nested struct schema
        - name: colume_a_1
          type: STRING
  cluster:
    using: [colume1]
  partition: # leave empty as {} to partition by ingestion time
    field: colume2 # column name
    type: day # day/hour, default: day
#    expiration: 24 # in hours
#    range:
#      start: 30
#      end: 60
#      interval: 2
#  expiration_time: 200 # in hours

```
This will add labels, description, schema, clustering, partition over colume2 by day
on the table once the `deploy` command is invoked.

Optimus generates specification on the root directory inside datastore with directory
name same as resource name, although you can change directory name to whatever you 
find fit to organize resources. Directory structures inside datastore doesn't 
matter as long as `resource.yaml` is in a unique directory. 

For example following is a valid directory structure
```shell
./
./bigquery/temporary-project/optimus-playground/resource.yaml
./bigquery/temporary-project/optimus-playground/first_table/resource.yaml
```

### Creating table over REST

Optimus exposes Create/Update rest APIS
```
Create: POST /api/v1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource
Update: PUT /api/v1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource
Read: GET /api/v1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource/{resource_name}
```

```json
{
  "resource": {
    "version": 1,
    "name": "temporary-project.optimus-playground.first_table",
    "datastore": "bigquery",
    "type": "table",
    "labels": {
      "usage": "testdataset",
      "owner": "optimus"
    },
    "spec": {
      "description": "example description",
      "schema": [
        {
          "name": "column1",
          "type": "INTEGER"
        },
        {
          "name": "column2",
          "type": "TIMESTAMP",
          "description": "example description",
          "mode": "required"
        },
        {
          "name": "column3",
          "type": "STRUCT",
          "schema": [
            {
              "name": "column_a_1",
              "type": "STRING"
            }
          ]
        }
      ],
      "partition": {
        "field": "column2"
      },
      "cluster": {
        "using": ["column1"]
      }
    }
  }
}
``` 

### Creating table over GRPC

Optimus in RuntimeService exposes an RPC 
```protobuf
rpc CreateResource(CreateResourceRequest) returns (CreateResourceResponse) {}

message CreateResourceRequest {
    string project_name = 1;
    string datastore_name = 2;
    ResourceSpecification resource = 3;
    string namespace = 4;
}
```
Function payload should be self-explanatory other than the struct `spec` part which
is very similar to how json representation look.

Spec will use `structpb` struct created with `map[string]interface{}`
For example:
```go
map[string]interface{
	"description": "example description",
	"schema": []interface{
	    map[string]interface{
	        "name": "colume1",
	        "type": "integer"
        },
        map[string]interface{
            "name": "colume2",
            "type": "timestamp"
            "description": "some description",
            "mode": "required"
        },
    },
	"partition": map[string]interface{
		"field": "column2"
    },
}
``` 
