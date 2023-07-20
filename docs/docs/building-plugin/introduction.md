# Introduction of Plugin Development

As mentioned in the [concepts](../concepts/plugin.md), plugins provide support for various data warehouses & any 
third party system to handle all the data transformations or movement. Before we start, let’s take a look at different 
implementations of plugin: YAML and binary.

## Yaml Implementation of Plugin
Most plugins are expected to implement just the info and project side use-cases (mentioned above) and these are 
data-driven i.e., plugin just provide data to Optimus. To simplify the development process of plugins, support for 
yaml mode of defining plugins is added.

```go
// representation of a yaml plugin schema in golang

// below struct definition in golang can be marshalled
// to generate yaml plugins

type YamlPlugin struct {
// info use-case
Name          string `yaml:"name"`
Description   string `yaml:"description"`
Plugintype    string `yaml:"plugintype"`
Pluginversion string `yaml:"pluginversion"`
Image         string `yaml:"image"`

    // survey use-case
    Questions     []struct {
        Name            string `yaml:"name"`
        Prompt          string `yaml:"prompt"`
        Help            string `yaml:"help"`
        Regexp          string `yaml:"regexp"`
        Validationerror string `yaml:"validationerror"`
        Minlength       int    `yaml:"minlength"`
        Required        bool     `yaml:"required,omitempty"`
        Maxlength       int    `yaml:"maxlength,omitempty"`
        Subquestions    []struct {
            Ifvalue   string `yaml:"ifvalue"`
            Questions []struct {
                Name        string   `yaml:"name"`
                Prompt      string   `yaml:"prompt"`
                Help        string   `yaml:"help"`
                Multiselect []string `yaml:"multiselect"`
                Regexp          string `yaml:"regexp"`
                Validationerror string `yaml:"validationerror"`
                Minlength       int    `yaml:"minlength"`
                Required        bool     `yaml:"required,omitempty"`
                Maxlength       int    `yaml:"maxlength,omitempty"`
            } `yaml:"questions"`
        } `yaml:"subquestions,omitempty"`
    } `yaml:"questions"`

    // default-static-values use-case
    Defaultassets []struct {
        Name  string `yaml:"name"`
        Value string `yaml:"value"`
    } `yaml:"defaultassets"`
    Defaultconfig []struct {
        Name  string `yaml:"name"`
        Value string `yaml:"value"`
    } `yaml:"defaultconfig"`
}
```

Refer to sample implementation here.


### Limitations of Yaml plugins:
Here the scope of YAML plugins is limited to driving surveys, providing default values for job config and assets, and 
providing plugin info. As the majority of the plugins are expected to implement a subset of these use cases, the 
support for YAML definitions for plugins is added which simplifies the development, packaging, and distribution of plugins.

For plugins that require enriching Optimus server-side behavior, YAML definitions fall short as this would require some code.

### Validating Yaml plugins:
Also support for validating yaml plugin is added into optimus. After creating yaml definitions of plugin, one can 
validate them as below:

```shell
optimus plugin validate --path {{directory of yaml plugins}}
```

** Note: The yaml plugin is expected to have file name as optimus-plugin-{{name}}.yaml
** Note: If Both yaml and binary plugin with same name are installed, Yaml implementation is prioritized over the 
corresponding counterparts in binary implementation.

## Binary Implementation of Plugin (to be deprecated)
Binary implementations of Plugins are binaries which implement predefined protobuf interfaces to extend Optimus 
functionalities and augment the yaml implementations with executable code. Binary Plugins are implemented using 
go-plugin developed by Hashicorp used in terraform and other similar products. Currently, Dependency Resolution Mod 
is the only interface that is supported in the binary approach to plugins.

Note : Binary plugins augment yaml plugins and they are not standalone.

Plugins can be implemented in any language as long as they can be exported as a single self-contained executable binary 
and implements a GRPC server. It is recommended to use Go currently for writing plugins because of its cross platform 
build functionality and to reuse protobuf sdk provided within Optimus core.

_Binary Plugins can potentially modify the behavior of Optimus in undesired ways. Exercise caution when adding new 
plugins developed by unrecognized developers._
