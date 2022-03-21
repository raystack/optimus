package config

type ProjectConfig struct {
	Version    int          `mapstructure:"version"`
	Log        LogConfig    `mapstructure:"log"`
	Host       string       `mapstructure:"host"` // optimus server host
	Project    Project      `mapstructure:"project"`
	Namespaces []*Namespace `mapstructure:"namespaces"`

	namespaceNameToNamespace map[string]*Namespace
}

type Datastore struct {
	Type   string            `mapstructure:"type"`   // type could be bigquery/postgres/gcs
	Path   string            `mapstructure:"path"`   // directory to find specifications
	Backup map[string]string `mapstructure:"backup"` // backup configuration
}

type Job struct {
	Path string `mapstructure:"path"` // directory to find specifications
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
