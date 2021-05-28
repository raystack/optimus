# Developing Optimus plugins

Optimus's responsibilities are currently divided in two parts, scheduling a 
transformation [task](../concepts/index.md#Job) and running one time action to 
create or modify a [datastore](../concepts/index.md#Datastore) resource. 
Defining how a datastore is managed can be easy and doesn't leave many options 
for configuration or ambiguity although the way datastores are implemented 
in Optimus gives developers flexibility to contribute more type of datastore, 
but it is not something we do every day.

Whereas tasks used in jobs that define how the transformation will execute, 
what configuration does it need as input from user, how does this task resolves
dependencies between each other, what kind of assets it might need. 
These questions are very open and answers to them could be different in 
different organization and users. To allow flexibility of answering these
questions by developers themselves, we have chosen to make it easy to 
contribute a new kind of task or even a hook. This modularity in Optimus
is achieved using plugins.

> Plugins are self-contained binaries which implements predefined protobuf interface
> to extend Optimus functionalities.

Optimus can be divided in two logical parts when we are thinking of a pluggable
model, one is the **core** where everything happens which is common for all 
job/datastore, and the other part which could be variable and needs user
specific definitions of how things should work which is a **plugin**.

## Creating a task plugin

At the moment Optimus supports task as well as hook plugins. In this section
we will be explaining how to write a new task although both are very similar.
Plugins are implemented using [go-plugin](https://github.com/hashicorp/go-plugin)
developed by Hashicorp used in terraform and other similar products. 

> Plugins can be implemented in any language as long as they can be exported as
> a single self-contained executable binary. 
 
It is recommended to use Golang currently for writing plugins because of its
cross platform build functionality and to reuse protobuf adapter provided 
within Optimus core. Even though the plugin is written in Golang, it could
be just a wrapper over what actually needs to be executed. Actual transformation
will be packed in a docker image and Optimus will execute any arbitrary 
docker image as long as it has access to reach container repository. 

> Task plugin itself is not executed for transformation but only used 
> for satisfying conditions which Optimus requires to be defined for each 
> Transformation task

To demonstrate this wrapping functionality, lets create a plugin in Golang and
use python for actual transformation logic.

You can choose to fork this [example](TODO)
template and modify it as per your needs or start fresh with creating an 
empty git repository.

// TODO

## Installing a plugin

> Plugins can potentially modify the behavior of Optimus in undesired ways. 
> Exercise caution when adding new plugins developed by unrecognized developers.
