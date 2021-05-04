package config

type Opctl struct {
	Version   int
	Host      string
	Job       Job
	Datastore []Datastore `yaml:"datastore"`
	Global    map[string]string

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
