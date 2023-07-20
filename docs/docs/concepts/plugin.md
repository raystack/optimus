# Plugin
Optimus can provide support for various data warehouses & any third party system to handle all the data transformations 
or movement through plugins. You can bring your own plugin by encapsulating all the logic in a docker container.

Currently, plugins can be defined as YAML or binary executables. YAML plugin provides the questionnaire and default 
values for job task’s / hook’s creation, as well as defines the image to execute. While a binary plugin, it is 
complementing the YAML plugin by providing support for automated dependency resolution.
