## Create Bigquery dataset

A dataset is contained within a specific Google project. Datasets are top-level 
containers that are used to organize and control access to your tables and views. 
A table or view must belong to a dataset, so you need to create at least one.

There are 3 ways to create a dataset:

### Creating dataset with Opctl

Supported datastore can be selected by calling
```bash
opctl create resource
```
Opctl will request a resource name which should be unique across whole datastore.
All resource specification contains a name field which conforms to a fixed format.
In case of bigquery dataset, format should be
`projectname.datasetname`.
After the name is provided, `Opctl` will create a file in configured datastore 
directory. Open the created specification file and add additional spec details
as follows:
```yaml
version: 1
name: temporary-project.optimus-playground
type: dataset
labels:
  usage: testdataset
  owner: optimus
spec:
  description: "example description"
  table_expiration: 24 # in hours
```
This will add labels, description and default table expiration(in hours) to dataset
once the `deploy` command is invoked.

### Creating dataset over REST

Optimus exposes Create/Update rest APIS
Create: POST /api/v1/project/{project_name}/datastore/{datastore_name}/resource
Update: PUT /api/v1/project/{project_name}/datastore/{datastore_name}/resource
Read: GET /api/v1/project/{project_name}/datastore/{datastore_name}/resource/{resource_name}

```json
{
    "resource": {
        "version": 1,
        "name" : "temporary-project.optimus-playground",
        "datastore" : "bigquery",
        "type" : "dataset",
        "labels": {
          "usage": "testdataset",
          "owner": "optimus"
        },
        "spec" : {
          "description": "example description",
          "table_expiration": 24
        }
    }
}
``` 

### Creating dataset over GRPC

Optimus in RuntimeService exposes an RPC 
```protobuf
rpc CreateResource(CreateResourceRequest) returns (CreateResourceResponse) {}

message CreateResourceRequest {
    string project_name = 1;
    string datastore_name = 2;
    ResourceSpecification resource = 3;
}
```
Function payload should be self-explanatory other than the struct `spec` part which
is very similar to how json representation look.

Spec will use `structpb` struct created with `map[string]interface{}`
For example:
```go
map[string]interface{
	"description": "example description",
	"table_expiration": 24
}
``` 