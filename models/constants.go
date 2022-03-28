package models

const (
	ProjectEntity   = "project"
	NamespaceEntity = "namespace"
	SecretEntity    = "secret"

	// the configs for use in macros inside task/hook config and job assets

	// ConfigKeyDstart start of the execution window
	ConfigKeyDstart = "DSTART"
	// ConfigKeyDend end of the execution window
	ConfigKeyDend = "DEND"
	// ConfigKeyExecutionTime time when the job started executing, this gets shared across all
	// task and hooks of a job instance
	ConfigKeyExecutionTime = "EXECUTION_TIME"
	// ConfigKeyDestination is destination urn
	ConfigKeyDestination = "JOB_DESTINATION"
)
