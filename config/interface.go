package config

type Provider interface {
	GetVersion() string
	GetHost() string
	GetJob() Job
	GetDatastore() []Datastore
	GetLog() LogConfig
	GetProject() Project
	GetNamespace() Namespace

	GetServe() ServerConfig
	GetScheduler() SchedulerConfig
	GetAdmin() AdminConfig
	GetTelemetry() TelemetryConfig
}
