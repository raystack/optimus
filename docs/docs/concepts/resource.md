# Resource

A resource is the representation of the warehouse unit that can be a source or a destination of a transformation job. 
The warehouse resources can be created, modified, and be read from Optimus, as well as can be backed up as requested. 
Each warehouse supports a fixed set of resource types and each type has its own specification schema. 
Optimus’ managed warehouse  is called Optimus datastore.

At the moment, Optimus supports BigQuery datastore for these type of resources:
- Dataset
- Table
- Standard View
- External Table

_Note: BigQuery resource deletion is currently not supported._
