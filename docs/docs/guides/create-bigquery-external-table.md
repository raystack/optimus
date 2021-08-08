---
id: create-bigquery-external-table
title: Create bigquery external table
---
A BigQuery external table is a data source stored in external storage that you can query directly
in BigQuery the same way you query a table. You can specify the schema of the external table when
it is created. At the moment only Google Drive source with Google Sheets format is supported.

There are 3 ways to create an external table:

### Creating external table with Optimus

Supported datastore can be selected by calling
```bash
optimus create resource
```
Optimus will request a resource name which should be unique across whole datastore.
All resource specification contains a name field which conforms to a fixed format.
In case of bigquery external table, format should be
`projectname.datasetname.tablename`.
After the name is provided, `optimus` will create a file in configured datastore 
directory. Open the created specification file and add additional spec details
as follows:
```yaml
version: 1
name: temporary-project.optimus-playground.first_table
type: external_table
labels:
  usage: testexternaltable
  owner: optimus
spec:
  description: "example description"
  schema:
    - name: colume1 
      type: INTEGER 
    - name: colume2
      type: TIMESTAMP
      description: "example field 2" 
  source:
    type: google_sheets
    uris:
      - https://docs.google.com/spreadsheets/d/spreadsheet_id 
    config:
      range: Sheet1!A1:B4 # Range of data to be ingested in format of [Sheet Name]![Cell Range]
      skip_leading_rows: 1 # Row of records to skip
```
This will add labels, description, schema, and external table source specification depending
on the type of external table. 

Optimus generates specification on the root directory inside datastore with directory
name same as resource name, although you can change directory name to whatever you 
find fit to organize resources. Directory structures inside datastore doesn't 
matter as long as `resource.yaml` is in a unique directory. 

For example following is a valid directory structure
```shell
./
./bigquery/temporary-project/optimus-playground/resource.yaml
./bigquery/temporary-project/optimus-playground/first_external_table/resource.yaml
```

### Creating external table over REST

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
    "type": "external_table",
    "labels": {
      "usage": "testexternaltable",
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
        }
      ],
      "source": {
        "type": "google_sheets",
        "uris": ["https://docs.google.com/spreadsheets/d/spreadsheet_id"],
        "config": {
          "range" : "Sheet1!A1:B4",
          "skip_leading_rows": 1
        }
      }
    }
  }
}
``` 

### Creating external table over GRPC

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
	"source": map[string]interface{
		"type": "google_sheets",
		"uris": []string{"https://docs.google.com/spreadsheets/d/spreadsheet_id"},
		"config": map[string]interface{
			"range": "Sheet1!A1:B4",
			"skip_leading_rows": 1
		}
    },
}
``` 
