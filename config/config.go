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
	Version int `mapstructure:"version"`
	// optimus server host
	Host string `mapstructure:"host"`

	Project   Project   `mapstructure:"project"`
	Namespace Namespace `mapstructure:"namespace"`

	Server    ServerConfig    `mapstructure:"serve"`
	Log       LogConfig       `mapstructure:"log"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Admin     AdminConfig     `mapstructure:"admin"`
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
}

type Datastore struct {
	// type could be bigquery/postgres/gcs
	Type string `mapstructure:"type"`

	// directory to find specifications
	Path string `mapstructure:"path"`

	// backup configuration
	Backup map[string]string `mapstructure:"backup"`
}

type Job struct {
	// directory to find specifications
	Path string `mapstructure:"path"`
}

type Project struct {
	Name   string            `mapstructure:"name"`
	Config map[string]string `mapstructure:"config"`
}

type Namespace struct {
	Name      string            `mapstructure:"name"`
	Config    map[string]string `mapstructure:"config"`
	Job       Job               `mapstructure:"job"`
	Datastore []Datastore       `mapstructure:"datastore"`
}

type LogConfig struct {
	// log level - debug, info, warning, error, fatal
	Level string `mapstructure:"level" default:"info"`

	// format strategy - plain, json
	Format string `mapstructure:"format"`
}

type ServerConfig struct {
	// port to listen on
	Port int `mapstructure:"port" default:"9100"`
	// the network interface to listen on
	Host string `mapstructure:"host" default:"0.0.0.0"`

	// service ingress host for jobs to communicate back to optimus
	IngressHost string `mapstructure:"ingress_host"`

	// random 32 character hash used for encrypting secrets
	AppKey string `mapstructure:"app_key"`

	DB                  DBConfig      `mapstructure:"db"`
	ReplayNumWorkers    int           `mapstructure:"replay_num_workers" default:"1"`
	ReplayWorkerTimeout time.Duration `mapstructure:"replay_worker_timeout" default:"120s"`
	ReplayRunTimeout    time.Duration `mapstructure:"replay_run_timeout"`
}

type DBConfig struct {
	// database connection string
	// e.g.: postgres://user:password@host:123/database?sslmode=disable
	DSN string `mapstructure:"dsn"`

	// maximum allowed idle DB connections
	MaxIdleConnection int `mapstructure:"max_idle_connection" default:"10"`

	// maximum allowed open DB connections
	MaxOpenConnection int `mapstructure:"max_open_connection" default:"20"`
}

type SchedulerConfig struct {
	Name     string `mapstructure:"name" default:"airflow2"`
	SkipInit bool   `mapstructure:"skip_init"`


	// RaftAddr used for leader consensus and maintaining RAFT log
	RaftAddr   string `mapstructure:"raft_addr"`

	// GossipAddr used for forming a cluster of nodes
	GossipAddr string `mapstructure:"gossip_addr"`

	// NodeID is a unique ID in a cluster, should be in `node-%d` format
	NodeID     string `mapstructure:"node_id"`

	// DataDir is used to keep raft WAL
	DataDir    string `mapstructure:"data_dir"`

	// Peers are comma delimited list of nodes this local node should search for starting up.
	// At least one is required to join a cluster. This will be empty for first node.
	Peers      string `mapstructure:"peers"`
}

type AdminConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type TelemetryConfig struct {
	ProfileAddr string `mapstructure:"profile_addr"`
	JaegerAddr  string `mapstructure:"jaeger_addr"`
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
	return o.Namespace.Job
}

func (o *Optimus) GetDatastore() []Datastore {
	return o.Namespace.Datastore
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
	return o.Telemetry
}
