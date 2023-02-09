package config

type ServerConfig struct {
	Version          Version           `mapstructure:"version"`
	Log              LogConfig         `mapstructure:"log"`
	Serve            Serve             `mapstructure:"serve"`
	Scheduler        SchedulerConfig   `mapstructure:"scheduler"`
	Telemetry        TelemetryConfig   `mapstructure:"telemetry"`
	ResourceManagers []ResourceManager `mapstructure:"resource_managers"`
	Plugin           PluginConfig      `mapstructure:"plugin"`
}

type Serve struct {
	Port        int      `mapstructure:"port" default:"9100"` // port to listen on
	IngressHost string   `mapstructure:"ingress_host"`        // service ingress host for jobs to communicate back to optimus
	AppKey      string   `mapstructure:"app_key"`             // random 32 character hash used for encrypting secrets
	DB          DBConfig `mapstructure:"db"`
}

type DBConfig struct {
	DSN               string `mapstructure:"dsn"`                              // data source name e.g.: postgres://user:password@host:123/database?sslmode=disable
	MinOpenConnection int    `mapstructure:"min_open_connection" default:"5"`  // minimum open DB connections
	MaxOpenConnection int    `mapstructure:"max_open_connection" default:"20"` // maximum allowed open DB connections
}

type SchedulerConfig struct {
	Name string `mapstructure:"name" default:"airflow"`
}

type TelemetryConfig struct {
	ProfileAddr string `mapstructure:"profile_addr"`
	JaegerAddr  string `mapstructure:"jaeger_addr"`
}

type ResourceManager struct {
	Name        string      `mapstructure:"name"`
	Type        string      `mapstructure:"type"`
	Description string      `mapstructure:"description"`
	Config      interface{} `mapstructure:"config"`
}

type ResourceManagerConfigOptimus struct {
	Host    string            `mapstructure:"host"`
	Headers map[string]string `mapstructure:"headers"`
}

type PluginConfig struct {
	Artifacts []string `mapstructure:"artifacts"`
}
