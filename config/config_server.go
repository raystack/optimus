package config

import (
	"time"
)

type ServerConfig struct {
	Version   Version         `mapstructure:"version"`
	Log       LogConfig       `mapstructure:"log"`
	Serve     Serve           `mapstructure:"serve"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
}

type Serve struct {
	Port        int      `mapstructure:"port" default:"9100"`    // port to listen on
	Host        string   `mapstructure:"host" default:"0.0.0.0"` // the network interface to listen on
	IngressHost string   `mapstructure:"ingress_host"`           // service ingress host for jobs to communicate back to optimus
	AppKey      string   `mapstructure:"app_key"`                // random 32 character hash used for encrypting secrets
	DB          DBConfig `mapstructure:"db"`
	Replay      Replay   `mapstructure:"replay"`
	Deployer    Deployer `mapstructure:"deployer"`
}

type Replay struct {
	NumWorkers    int           `mapstructure:"num_workers" default:"1"`
	WorkerTimeout time.Duration `mapstructure:"worker_timeout" default:"120s"`
	RunTimeout    time.Duration `mapstructure:"run_timeout"`
}

type Deployer struct {
	NumWorkers    int           `mapstructure:"num_workers" default:"1"`
	WorkerTimeout time.Duration `mapstructure:"worker_timeout" default:"300m"`
	QueueCapacity int           `mapstructure:"queue_capacity" default:"10"`
}

type DBConfig struct {
	DSN               string `mapstructure:"dsn"`                              // data source name e.g.: postgres://user:password@host:123/database?sslmode=disable
	MaxIdleConnection int    `mapstructure:"max_idle_connection" default:"10"` // maximum allowed idle DB connections
	MaxOpenConnection int    `mapstructure:"max_open_connection" default:"20"` // maximum allowed open DB connections
}

type SchedulerConfig struct {
	Name       string `mapstructure:"name" default:"airflow"`
	SkipInit   bool   `mapstructure:"skip_init"`
	RaftAddr   string `mapstructure:"raft_addr"`
	GossipAddr string `mapstructure:"gossip_addr"`
	NodeID     string `mapstructure:"node_id"`
	DataDir    string `mapstructure:"data_dir"`
	Peers      string `mapstructure:"peers"`
}

type TelemetryConfig struct {
	ProfileAddr string `mapstructure:"profile_addr"`
	JaegerAddr  string `mapstructure:"jaeger_addr"`
}
