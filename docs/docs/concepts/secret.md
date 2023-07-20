# Secret
A lot of transformation operations require credentials to execute. These credentials (secrets) are needed in some tasks, 
hooks, and may also be needed in deployment processes such as dependency resolution. Optimus provides a convenient way 
to store secrets and make them accessible in containers during the execution.

You can easily create, update, and delete your own secrets using CLI or REST API. Secrets can be created at a project 
level which is accessible from all the namespaces in the project, or can just be created at the namespace level. These 
secrets will then can be used as part of the job spec configuration using macros with their names. Only the secrets 
created at the project & namespace the job belongs to can be referenced.
