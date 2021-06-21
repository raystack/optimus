package config

import (
	"strings"

	"github.com/knadh/koanf"
)

var (
	KeyVersion = "version"
	KeyHost    = "host"

	KeyJobPath = "job.path"

	KeyDatastoreName = "datastore.name"
	KeyDatastorePath = "datastore.path"

	KeyProjectConfigGlobal = "config.global"
	KeyProjectConfigLocal  = "config.local"

	KeyLogLevel  = "log.level"
	KeyLogFormat = "log.format"

	KeyServeHost                    = "serve.host"
	KeyServePort                    = "serve.port"
	KeyServeAppKey                  = "serve.app_key"
	KeyServeIngressHost             = "serve.ingress_host"
	KeyServeDBDSN                   = "serve.db.dsn"
	KeyServeDBMaxIdleConnection     = "serve.db.max_idle_connection"
	KeyServeDBMaxOpenConnection     = "serve.db.max_open_connection"
	KeyServeMetadataWriterBatchSize = "serve.metadata.writer_batch_size"
	KeyServeMetadataKafkaBrokers    = "serve.metadata.kafka_brokers"
	KeyServeMetadataKafkaJobTopic   = "serve.metadata.kafka_job_topic"
	KeyServeMetadataKafkaBatchSize  = "serve.metadata.kafka_batch_size"

	KeySchedulerName = "scheduler.name"

	KeyAdminEnabled = "admin.enabled"
)

type Optimus struct {
	// Note: don't access configs using these member variables, instead use methods
	// they are here to use yaml marshaller and generate basic config file

	// configuration version
	Version int `yaml:"version"`
	// optimus server host
	Host string `yaml:"host"`

	Job       Job           `yaml:"job"`
	Datastore []Datastore   `yaml:"datastore"`
	Config    ProjectConfig `yaml:"config"`

	k      *koanf.Koanf
	parser koanf.Parser
}

type Datastore struct {
	// type could be bigquery/postgres/gcs
	Type string `yaml:"type" koanf:"type"`

	// directory to find specifications
	Path string `yaml:"path" koanf:"path"`
}

type Job struct {
	// directory to find specifications
	Path string `yaml:"path"`
}

type ProjectConfig struct {
	// per project
	Global map[string]string `yaml:"global"`

	// per namespace
	Local map[string]string `yaml:"local"`
}

type LogConfig struct {
	// log level - debug, info, warning, error, fatal
	Level string `yaml:"level"`

	// format strategy - plain, json
	Format string `yaml:"format"`
}

type ServerConfig struct {
	// port to listen on
	Port int `yaml:"port"`
	// the network interface to listen on
	Host string `yaml:"host"`

	// service ingress host for jobs to communicate back to optimus
	IngressHost string `yaml:"ingress_host"`

	// random 32 character hash used for encrypting secrets
	AppKey string `yaml:"app_key"`

	DB           DBConfig       `yaml:"db"`
	Metadata     MetadataConfig `yaml:"metadata"`
	JobQueueSize int            `yaml:"job_queue_size"`
}

type DBConfig struct {
	// database connection string
	// e.g.: postgres://user:password@host:123/database?sslmode=disable
	DSN string `yaml:"host"`

	// maximum allowed idle DB connections
	MaxIdleConnection int `yaml:"max_idle_connection"`

	// maximum allowed open DB connections
	MaxOpenConnection int `yaml:"max_open_connection"`
}

type MetadataConfig struct {
	// limit on how many messages will be buffered before being sent to a writer
	WriterBatchSize int `yaml:"writer_batch_size"`

	// kafka topic where metadata of optimus Job needs to be published
	KafkaJobTopic string `yaml:"kafka_job_topic"`

	// comma separated kafka brokers to use for publishing metadata, leave empty to disable metadata publisher
	KafkaBrokers string `yaml:"kafka_brokers"`

	// limit on how many messages will be buffered before being sent to a kafka partition
	KafkaBatchSize int `yaml:"kafka_batch_size"`
}

type SchedulerConfig struct {
	Name string `yaml:"name"`
}

type AdminConfig struct {
	Enabled bool `yaml:"enabled"`
}

func (o Optimus) GetVersion() string {
	return o.k.String(KeyVersion)
}

func (o Optimus) GetProjectConfig() ProjectConfig {
	return ProjectConfig{
		Global: o.k.StringMap(KeyProjectConfigGlobal),
		Local:  o.k.StringMap(KeyProjectConfigLocal),
	}
}

func (o Optimus) GetHost() string {
	return o.k.String(KeyHost)
}

func (o Optimus) GetJob() Job {
	return Job{
		Path: o.k.String(KeyJobPath),
	}
}

func (o Optimus) GetDatastore() []Datastore {
	ds := []Datastore{}
	_ = o.k.Unmarshal("datastore", &ds)
	return ds
}

func (o Optimus) GetLog() LogConfig {
	return LogConfig{
		Level:  o.k.String(KeyLogLevel),
		Format: o.k.String(KeyLogFormat),
	}
}

func (o Optimus) GetServe() ServerConfig {
	return ServerConfig{
		Port:        o.k.Int(KeyServePort),
		Host:        o.k.String(KeyServeHost),
		IngressHost: o.eKs(KeyServeIngressHost),
		AppKey:      o.eKs(KeyServeAppKey),
		DB: DBConfig{
			DSN:               o.k.String(KeyServeDBDSN),
			MaxIdleConnection: o.eKi(KeyServeDBMaxIdleConnection),
			MaxOpenConnection: o.eKi(KeyServeDBMaxOpenConnection),
		},
		Metadata: MetadataConfig{
			WriterBatchSize: o.eKi(KeyServeMetadataWriterBatchSize),
			KafkaJobTopic:   o.eKs(KeyServeMetadataKafkaJobTopic),
			KafkaBrokers:    o.eKs(KeyServeMetadataKafkaBrokers),
			KafkaBatchSize:  o.eKi(KeyServeMetadataKafkaBatchSize),
		},
	}
}

func (o Optimus) GetScheduler() SchedulerConfig {
	return SchedulerConfig{
		Name: o.k.String(KeySchedulerName),
	}
}

func (o Optimus) GetAdmin() AdminConfig {
	return AdminConfig{
		Enabled: o.k.Bool(KeyAdminEnabled),
	}
}

// eKs replaces . with _ to support buggy koanf config loader from ENV
// this should be used in all keys where underscore is used
func (o Optimus) eKs(e string) string {
	// read with default key - used in config file
	res := o.k.String(e)

	// read with replaced key - used in env
	if v := o.k.String(strings.Replace(e, "_", ".", -1)); v != "" {
		res = v
	}
	return res
}

// eKi replaces . with _ to support buggy koanf config loader from ENV
// this should be used in all keys where underscore is used
func (o Optimus) eKi(e string) int {
	// read with default key - used in config file
	res := o.k.Int(e)

	// read with replaced key - used in env
	if v := o.k.Int(strings.Replace(e, "_", ".", -1)); v != 0 {
		res = v
	}
	return res
}
