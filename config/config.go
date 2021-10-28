package config

import (
	"strconv"
	"time"
)

var (
	KeyServeReplayNumWorkers = "serve.replay_num_workers"
)

type Optimus struct {
	// configuration version
	Version int `yaml:"version" mapstructure:"version"`
	// optimus server host
	Host string `yaml:"host" mapstructure:"host"`

	Project   Project   `yaml:"project" mapstructure:"project"`
	Namespace Namespace `yaml:"namespace" mapstructure:"namespace"`

	Job       Job         `yaml:"job" mapstructure:"job"`
	Datastore []Datastore `yaml:"datastore" mapstructure:"datastore"`

	Server    ServerConfig    `mapstructure:"serve"`
	Log       LogConfig       `mapstructure:"log"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Admin     AdminConfig     `mapstructure:"admin"`
}

type Datastore struct {
	// type could be bigquery/postgres/gcs
	Type string `yaml:"type" mapstructure:"type" koanf:"type"`

	// directory to find specifications
	Path string `yaml:"path" mapstructure:"path" koanf:"path"`

	// backup configuration
	Backup map[string]string `yaml:"path" mapstructure:"backup" koanf:"backup"`
}

type Job struct {
	// directory to find specifications
	Path string `yaml:"path" mapstructure:"path"`
}

type Project struct {
	Name   string            `yaml:"name" mapstructure:"name"`
	Config map[string]string `yaml:"config" mapstructure:"config"`
}

type Namespace struct {
	Name   string            `yaml:"name" mapstructure:"name"`
	Config map[string]string `yaml:"config" mapstructure:"config"`
}

type LogConfig struct {
	// log level - debug, info, warning, error, fatal
	Level string `yaml:"level" mapstructure:"level"`

	// format strategy - plain, json
	Format string `yaml:"format" mapstructure:"format"`
}

type ServerConfig struct {
	// port to listen on
	Port int `yaml:"port" mapstructure:"port" default:"9100"`
	// the network interface to listen on
	Host string `yaml:"host" mapstructure:"host" default:"0.0.0.0"`

	// service ingress host for jobs to communicate back to optimus
	IngressHost string `yaml:"ingress_host" mapstructure:"ingress_host"`

	// random 32 character hash used for encrypting secrets
	AppKey string `yaml:"app_key" mapstructure:"app_key"`

	DB                      DBConfig       `yaml:"db" mapstructure:"db"`
	Metadata                MetadataConfig `yaml:"metadata" mapstructure:"metadata"`
	ReplayNumWorkers        int            `yaml:"replay_num_workers" mapstructure:"replay_num_workers" default:"1"`
	ReplayWorkerTimeoutSecs time.Duration  `yaml:"replay_worker_timeout_secs" mapstructure:"replay_worker_timeout_secs" default:"120s"`
	ReplayRunTimeoutSecs    time.Duration  `yaml:"replay_run_timeout_secs" mapstructure:"replay_run_timeout_secs"`
}

type DBConfig struct {
	// database connection string
	// e.g.: postgres://user:password@host:123/database?sslmode=disable
	DSN string `yaml:"host" mapstructure:"dsn"`

	// maximum allowed idle DB connections
	MaxIdleConnection int `yaml:"max_idle_connection" mapstructure:"max_idle_connection" default:"5"`

	// maximum allowed open DB connections
	MaxOpenConnection int `yaml:"max_open_connection" mapstructure:"max_open_connection" default:"10"`
}

type MetadataConfig struct {
	// limit on how many messages will be buffered before being sent to a writer
	WriterBatchSize int `yaml:"writer_batch_size" mapstructure:"writer_batch_size" default:"50"`

	// kafka topic where metadata of optimus Job needs to be published
	KafkaJobTopic string `yaml:"kafka_job_topic" mapstructure:"kafka_job_topic" default:"resource_optimus_job_log"`

	// comma separated kafka brokers to use for publishing metadata, leave empty to disable metadata publisher
	KafkaBrokers string `yaml:"kafka_brokers" mapstructure:"kafka_brokers"`

	// limit on how many messages will be buffered before being sent to a kafka partition
	KafkaBatchSize int `yaml:"kafka_batch_size" mapstructure:"kafka_batch_size" default:"50"`
}

type SchedulerConfig struct {
	Name     string `yaml:"name" mapstructure:"name" default:"airflow2"`
	SkipInit bool   `yaml:"skip_init" mapstructure:"skip_init"`

	RaftAddr   string `yaml:"raft_addr" mapstructure:"raft_addr"`
	GossipAddr string `yaml:"gossip_addr" mapstructure:"gossip_addr"`
	NodeID     string `yaml:"node_id" mapstructure:"node_id"`
	DataDir    string `yaml:"data_dir" mapstructure:"data_dir"`
	Peers      string `yaml:"peers" mapstructure:"peers"`
}

type AdminConfig struct {
	Enabled bool `yaml:"enabled" mapstructure:"enabled"`
}

type TelemetryConfig struct {
	ProfileAddr string `yaml:"profile_addr"`
	JaegerAddr  string `yaml:"jaeger_addr"`
}

func (o *Optimus) GetVersion() string {
	return strconv.Itoa(o.Version)
}

func (o *Optimus) GetHost() string {
	return o.Host
}

func (o *Optimus) GetProject() Project {
	return o.Project
}

func (o *Optimus) GetNamespace() Namespace {
	return o.Namespace
}

func (o *Optimus) GetJob() Job {
	return o.Job
}

func (o *Optimus) GetDatastore() []Datastore {
	return o.Datastore
}

func (o *Optimus) GetLog() LogConfig {
	return o.Log
}

func (o *Optimus) GetServe() ServerConfig {
	return o.Server
}

func (o *Optimus) GetScheduler() SchedulerConfig {
	return o.Scheduler
}

func (o *Optimus) GetAdmin() AdminConfig {
	return o.Admin
}

func (o *Optimus) GetTelemetry() TelemetryConfig {
	return TelemetryConfig{
		ProfileAddr: o.GetTelemetry().ProfileAddr,
		JaegerAddr:  o.GetTelemetry().JaegerAddr,
	}
}
