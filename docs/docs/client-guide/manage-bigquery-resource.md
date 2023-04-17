# Manage BigQuery Resource

Below is the list of the resource types that Optimus supported:

| Type           | Description                                                                                                        |
|----------------|--------------------------------------------------------------------------------------------------------------------|
| dataset        | Resource name format: [project].[dataset] <br/> Spec can includes: table_expiration, description                        |
| table          | Resource name format: [project].[dataset].[table] <br/> Spec can includes: schema, partition, cluster, description |
| view           | Resource name format: [project].[dataset].[view] <br/> Spec can includes: view_query, description                  |
| external_table | Resource name format: [project].[dataset].[table] <br/> Spec can include: schema, source, description              |

You can create any of the above jobs using the same following format:
```shell
$ optimus resource create
```

Make sure to put the correct resource type as you are intended. Once you fill in the command prompt questions, Optimus will create a file (resource.yaml) in the configured datastore directory. Below is an example of each of the type’s resource specifications.

## Dataset
```yaml
version: 1
name: sample-project.playground
type: dataset
labels:
  usage: documentation
  owner: optimus
spec:
  description: "example description"
  table_expiration: 24 # in hours
```

## Table
```yaml
version: 1
name: sample-project.playground.sample_table
type: table
labels:
  usage: documentation
  owner: optimus
spec:
  description: "example description"
  schema:
  - name: column1
    type: INTEGER
  - name: column2
    type: TIMESTAMP
    description: "example field 2"
    mode: required # (repeated/required/nullable), default: nullable
  - name: column3
    type: STRUCT
    schema: # nested struct schema
  - name: column_a_1
    type: STRING
  cluster:
    using: [column1]
  partition: # leave empty as {} to partition by ingestion time
    field: column2 # column name
    type: day # day/hour, default: day
```



## View
```yaml
version: 1
name: sample-project.playground.sample_view
type: view
labels:
  usage: documentation
  owner: optimus
spec:
  description: "example description"
  view_query: |
    Select * from sample-project.playground.sample_table
```


## External Table
```yaml
version: 1
name: sample-project.playground.sample_table
type: external_table
labels:
  usage: documentation
  owner: optimus
spec:
  description: "example description"
  schema:
  - name: column1
    type: INTEGER
  - name: column2
    type: TIMESTAMP
    description: "example field 2"
  source:
    type: google_sheets
  uris:
  - https://docs.google.com/spreadsheets/d/spreadsheet_id
  config:
    range: Sheet1!A1:B4 # Range of data to be ingested in the format of [Sheet Name]![Cell Range]
    skip_leading_rows: 1 # Row of records to skip
```

## Upload Resource Specifications
Once the resource specifications are ready, you can upload all resource specifications using the below command:
```shell
$ optimus resource upload-all --verbose
```


The above command will try to compare the incoming resources to the existing resources in the server. It will create 
a new resource if it does not exist yet, and modify it if exists, but will not delete any resources. Optimus does not 
support BigQuery resource deletion nor the resource record in the Optimus server itself yet.
