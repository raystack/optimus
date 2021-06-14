package config

type Provider interface {
	GetVersion() string
	GetHost() string
	GetJob() Job
	GetDatastore() []Datastore
	GetProjectConfig() ProjectConfig
	GetLog() LogConfig

	GetServe() ServerConfig
	GetScheduler() SchedulerConfig
	GetAdmin() AdminConfig
}
