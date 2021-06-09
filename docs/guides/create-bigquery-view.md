## Create Bigquery view

A view is a virtual table defined by a SQL query. When you create a view, 
you query it in the same way you query a table. When a user queries the view, 
the query results contain data only from the tables and fields specified in the 
query that defines the view.
At the moment only standard view is supported.

There are 3 ways to create a view:

### Creating table with Optimus

Supported datastore can be selected by calling
```bash
optimus create resource
```
Optimus will request a resource name which should be unique across whole datastore.
All resource specification contains a name field which conforms to a fixed format.
In case of bigquery view, format should be
`projectname.datasetname.viewname`.
After the name is provided, `optimus` will create a file in configured datastore 
directory. Open the created specification file and add additional spec details
as follows:
```yaml
version: 1
name: temporary-project.optimus-playground.first_view
type: view
labels:
  usage: testview
  owner: optimus
spec:
  description: "example description"
  view_query: |
    Select * from temporary-project.optimus-playground.first_table
```
This will add labels, description, along with the query for view once the 
`deploy` command is invoked.
To use text editor intellisense for SQL formatting and linting, view query can 
also be added in a separate file inside the same directory with the name `view.sql`.
Directory will look something like:
```shell
./
./bigquery/temporary-project.optimus-playground.first_view/resource.yaml
./bigquery/temporary-project.optimus-playground.first_view/view.sql
```
Remove the `view_query` field from the resource specification if the query is
specified in a seperate file.

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
    "name": "temporary-project.optimus-playground.first_view",
    "datastore": "bigquery",
    "type": "view",
    "labels": {
      "usage": "testview",
      "owner": "optimus"
    },
    "spec": {
      "description": "example description",
      "view_query": "Select * from temporary-project.optimus-playground.first_table"
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
	"view_query": "Select * from temporary-project.optimus-playground.first_table"
}
``` 