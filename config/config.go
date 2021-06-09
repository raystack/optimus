package config

type Optimus struct {
	// configuration version
	Version int `yaml:"version" koanf:"version"`

	// TODO: Add a project name field once we have auth
	// Name string

	// optimus server host
	Host string `yaml:"host" koanf:"host"`

	Job       Job           `yaml:"job" koanf:"job"`
	Datastore []Datastore   `yaml:"datastore" koanf:"datastore"`
	Config    ProjectConfig `yaml:"config" koanf:"config"`

	Log       LogConfig       `yaml:"log" koanf:"log"`
	Serve     ServerConfig    `yaml:"serve" koanf:"serve"`
	Scheduler SchedulerConfig `yaml:"scheduler" koanf:"scheduler"`
	Admin     AdminConfig     `yaml:"admin" koanf:"admin"`
}

type Datastore struct {
	// type could be bigquery/postgres/gcs
	Type string `yaml:"type" koanf:"type"`

	// directory to find specifications
	Path string `yaml:"path" koanf:"path"`
}

type Job struct {
	// directory to find specifications
	Path string `yaml:"path" koanf:"path"`
}

type ProjectConfig struct {
	// per project
	Global map[string]string `yaml:"global" koanf:"global"`

	// per namespace
	Local map[string]string `yaml:"local" koanf:"path"`
}

type LogConfig struct {
	// log level - debug, info, warning, error, fatal
	Level string `yaml:"level" koanf:"level"`

	// format strategy - plain, json
	Format string `yaml:"format" koanf:"format"`
}

type ServerConfig struct {
	// port to listen on
	Port int `yaml:"port" koanf:"port"`
	// the network interface to listen on
	Host string `yaml:"host" koanf:"host"`

	// service ingress host for jobs to communicate back to optimus
	IngressHost string `yaml:"ingress_host" koanf:"ingress_host"`

	// random 32 character hash used for encrypting secrets
	AppKey string `yaml:"app_key" koanf:"app_key"`

	DB       DBConfig       `yaml:"db" koanf:"db"`
	Metadata MetadataConfig `yaml:"metadata" koanf:"metadata"`
}

type DBConfig struct {
	// database connection string
	// e.g.: postgres://user:password@host:123/database?sslmode=disable
	DSN string `yaml:"host" koanf:"dsn"`

	// maximum allowed idle DB connections
	MaxIdleConnection int `yaml:"max_idle_connection"`

	// maximum allowed open DB connections
	MaxOpenConnection int `yaml:"max_open_connection"`
}

type MetadataConfig struct {
	// limit on how many messages will be buffered before being sent to a writer
	WriterBatchSize int

	// kafka topic where metadata of optimus Job needs to be published
	KafkaJobTopic string

	// comma separated kafka brokers to use for publishing metadata, leave empty to disable metadata publisher
	KafkaBrokers string `yaml:"kafka_brokers"`

	// limit on how many messages will be buffered before being sent to a kafka partition
	KafkaBatchSize int
}

type SchedulerConfig struct {
	Name string `yaml:"name"`
}

type AdminConfig struct {
	Enabled bool `yaml:"enabled"`
}
