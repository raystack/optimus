package config

import (
	"encoding/json"
	"strings"
	"time"

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
	KeyServeReplayNumWorkers        = "serve.replay_num_workers"
	KeyServeReplayWorkerTimeoutSecs = "serve.replay_worker_timeout_secs"
	KeyServeReplayRunTimeoutSecs    = "serve.replay_run_timeout_secs"

	KeySchedulerName       = "scheduler.name"
	KeySchedulerSkipInit   = "scheduler.skip_init"
	KeySchedulerRaftAddr   = "scheduler.raft_addr"
	KeySchedulerGossipAddr = "scheduler.gossip_addr"
	KeySchedulerNodeID     = "scheduler.node_id"
	KeySchedulerDataDir    = "scheduler.data_dir"
	KeySchedulerPeers      = "scheduler.peers"

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

	// backup configuration
	Backup map[string]string `yaml:"backup" koanf:"backup"`
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

	DB                      DBConfig       `yaml:"db"`
	Metadata                MetadataConfig `yaml:"metadata"`
	ReplayNumWorkers        int            `yaml:"replay_num_workers"`
	ReplayWorkerTimeoutSecs time.Duration  `yaml:"replay_worker_timeout_secs"`
	ReplayRunTimeoutSecs    time.Duration  `yaml:"replay_run_timeout_secs"`
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
	Name     string `yaml:"name"`
	SkipInit bool   `yaml:"skip_init"`

	RaftAddr   string `yaml:"raft_addr"`
	GossipAddr string `yaml:"gossip_addr"`
	NodeID     string `yaml:"node_id"`
	DataDir    string `yaml:"data_dir"`
	Peers      string `yaml:"peers"`
}

type AdminConfig struct {
	Enabled bool `yaml:"enabled"`
}

func (o *Optimus) GetVersion() string {
	return o.eKs(KeyVersion)
}

func (o *Optimus) GetProjectConfig() ProjectConfig {
	return ProjectConfig{
		Global: o.k.StringMap(KeyProjectConfigGlobal),
		Local:  o.k.StringMap(KeyProjectConfigLocal),
	}
}

func (o *Optimus) GetHost() string {
	return o.eKs(KeyHost)
}

func (o *Optimus) GetJob() Job {
	return Job{
		Path: o.eKs(KeyJobPath),
	}
}

func (o *Optimus) GetDatastore() []Datastore {
	ds := []Datastore{}
	if o.k.Get("datastore") != nil {
		err := o.k.Unmarshal("datastore", &ds)
		if err != nil {
			// env var loaded config is in string
			json.Unmarshal(o.k.Bytes("datastore"), &ds)
		}
	}
	return ds
}

func (o *Optimus) GetLog() LogConfig {
	return LogConfig{
		Level:  o.eKs(KeyLogLevel),
		Format: o.eKs(KeyLogFormat),
	}
}

func (o *Optimus) GetServe() ServerConfig {
	return ServerConfig{
		Port:        o.eKi(KeyServePort),
		Host:        o.eKs(KeyServeHost),
		IngressHost: o.eKs(KeyServeIngressHost),
		AppKey:      o.eKs(KeyServeAppKey),
		DB: DBConfig{
			DSN:               o.eKs(KeyServeDBDSN),
			MaxIdleConnection: o.eKi(KeyServeDBMaxIdleConnection),
			MaxOpenConnection: o.eKi(KeyServeDBMaxOpenConnection),
		},
		Metadata: MetadataConfig{
			WriterBatchSize: o.eKi(KeyServeMetadataWriterBatchSize),
			KafkaJobTopic:   o.eKs(KeyServeMetadataKafkaJobTopic),
			KafkaBrokers:    o.eKs(KeyServeMetadataKafkaBrokers),
			KafkaBatchSize:  o.eKi(KeyServeMetadataKafkaBatchSize),
		},
		ReplayNumWorkers:        o.eKi(KeyServeReplayNumWorkers),
		ReplayWorkerTimeoutSecs: time.Second * time.Duration(o.eKi(KeyServeReplayWorkerTimeoutSecs)),
		ReplayRunTimeoutSecs:    time.Second * time.Duration(o.eKi(KeyServeReplayRunTimeoutSecs)),
	}
}

func (o *Optimus) GetScheduler() SchedulerConfig {
	return SchedulerConfig{
		Name:       o.eKs(KeySchedulerName),
		SkipInit:   o.k.Bool(KeySchedulerSkipInit),
		RaftAddr:   o.eKs(KeySchedulerRaftAddr),
		GossipAddr: o.eKs(KeySchedulerGossipAddr),
		NodeID:     o.eKs(KeySchedulerNodeID),
		DataDir:    o.eKs(KeySchedulerDataDir),
		Peers:      o.eKs(KeySchedulerPeers),
	}
}

func (o *Optimus) GetAdmin() AdminConfig {
	return AdminConfig{
		Enabled: o.k.Bool(KeyAdminEnabled),
	}
}

// eKs replaces . with _ to support buggy koanf config loader from ENV
// this should be used in all keys where underscore is used
func (o *Optimus) eKs(e string) string {
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
