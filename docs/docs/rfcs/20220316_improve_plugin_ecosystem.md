- Feature Name: Improve Plugin Ecosystem
- Status: Draft
- Start Date: 2022-03-16
- Authors: 

# Summary

With the current plugin ecosystem serving the purpose, this document is aimed to relook at the initial decisions, look at the areas we haven't considered earlier and question ourselves if they still hold good or can be simplified/enhanced to meet the growing needs.

# Technical Design

## Areas of Consideration :

1. Ease Plugin Installation
2. Relook at Current Contracts
3. Avoid the necessity of installation of plugins at the server & client side.
4. Ease creation of plugin
5. Streamline plugin secrets usage.

## Proposed Changes :

### Summary

1. A plugin can be flexible to act as a hook or a task.
2. A plugin doesn't need to have its dependencies fixed in the code, a user can provide dependencies between plugins optionally.
3. Dependencies between pre hooks and post hooks can be specified by user in the job specification.
4. Plugin Installation should be driven through Optimus yaml.
5. For better CLI experience instead of relying on the Plugin, let optimus server provide the questions to the user to avoid installation of plugins in local which gives more troubles in maintaining the same versions b/w server & client.
6. Plugin can define the expected mandatory configs including secrets in the specification which can help in validation of the specs.
7. Api_version in PluginInfo and dry_run in plugin_options are not needed & used at the moment so they can be removed.

### A plugin can be flexible to act as a hook or a task.

Currently, we label a task or a hook when we create the plugin, this is not needed as a plugin can act as a hook or a task it is upto the user on how to use it. But, if the DependencyMod has been implemented by the plugin then it will be provided to the user as task only and will not be allowed by the system if configured as hook. 

### Plugin Dependencies

By default a task depends on all pre hooks and all post hooks depends on task. If there are any extra dependencies between hooks then they can be defined in the project specification as it is highly possible that different users have different ways of configuring the dependencies. So, project admin can define the extra dependencies if needed through optimus.yaml or can provide it through project registration api call. These dependencies can be explicitly overridden by the user in the job_spec; job_spec will have extra `depends_on` field which can be overridden to specify the comma seperated names of the hooks which it depends on.

``` yaml
plugins :
	dependencies :
		- post_hook_alpha >> post_hook_beta
		- pre_hook_alpha >> pre_hook_beta,pre_hook_gamma
```

### Support Multiple Plugins of Same Type

Currently there is a support of adding a plugin of a single type, but there is a possibility of adding multiple plugins of same type which there is no need to limit. If that support need to be provided then in the specification user need to provide the `name` and `type` in the specification. For all the existing specs, if name is only provided then it becomes the type. For the newer specifications it is advised to specify the type in addition to `name`.

### Plugin Installation

Instead of installing the plugins outside the system, let optimus binary installs the plugins based on the specification. Optimus to support installing plugins from a http url, file url, s3 or gcs. There can be plugin providers which provides the discovery of the plugins and server can authenticate to the providers based on the details provided. The plugin name is what is shown to the end user instead of what is defined in the plugin_info response, this will help in avoiding any name collisions of plugins.

Optimus expects the url can be of zip, tar.gz or a binary itself and it expects a single binary which will be renamed with the name specified and installed in the location specified.

```yaml
plugins :
 providers :
 - http :
    name :
 		url :
 - gcs :
    name :
 		location :
 		service_account : <base64_encoded_service_account>
 install_location : <local_path>
 list :
 - plugin :
 			name : 
 			url : http://<internal_url>/<plugin_name>.tar.gz
 - plugin :
 			name : 
 			url : gcs://<bucket>/<plugin_name>.zip
 - plugin :
      provider : 
 			name : 
 			url : gcs://<bucket>/<plugin_name>.exe
 
```



### Avoid Installation of Plugins on client side

Plugins can be avoided to be installed in client side as its installation is an extra step for the user and it is easy for server & client to go out of sync. As a good practice it is always better to keep the client lite. Currently, the plugins are serving questions on the client side which optimus server has to handle. Instead of Optimus CLI getting the survery questions from the Plugin, let the questions come from server. There are two improvements that I believe we can do here.

1. Optimus CLI can get the survery questions from the server and the server fetches the questions by talking to the plugin.
2. Plugin can just define the questions in YAML along with all the validations and Optimus CLI can use the YAML served by the server to do the survey. Each plugin as part of handshake provides the YAML with all the questions. Defining the spec in YAML is more cleaner to read than defining via code. 

```yaml
//TODO the survey specification
```



### Mandatory configs of Plugins

I don't think it is a fair assumption that we will have questions for all the required configurations of plugins. So it is better for plugin to define what all are the required configs in the plugin specification which will be used to validate if user didn't provide any of the required configs. By default all the required configs should be specified in the specification as a template through CLI and expect user to update the same.

### Other Improvements

1. Plugin infö has `api_version` and `dry_run` I belive they are not used and needed at the moment and can be removed to avoid any confusion. 
2. Any plugin secret will be provided through JobSpec during dependency resolution or execution, this is the only mode of input a user can provide to the plugin and the executor image.

### Standardize Plugin Development And Deployment

An Optimus plugin has two parts

1. The executor image which is run on the kubernetes.
2. The core plugin which defines the specification defined by the optimus.

The executor image just needs all the asset files and configurations as environment variables, it doesn't need to have any context of Optimus. So, we expect plugin developers to just define the executor image version. Optimus builds a wrapper image using with the base_image which has the entrypoint.sh instead of leaving the responsibility to the user which enables the optimus admin to upgrade the wrapper images in a centralized fashion as it is coupled with the server upgrade. The version of the wrapper image will be upgraded when either of executor image version has changed or the base image version.

