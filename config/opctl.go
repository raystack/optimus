package config

type Opctl struct {
	Version   int
	Host      string
	Job       Job
	Datastore []Datastore `yaml:"datastore"`
	Config    ConfigSpec  `yaml:"config"`

	// TODO: Add a name field once we have auth
	// Name string
}

type Datastore struct {
	Type string
	Path string
}

type Job struct {
	Path string
}

type ConfigSpec struct {
	Global map[string]string
	Local  map[string]string
}
