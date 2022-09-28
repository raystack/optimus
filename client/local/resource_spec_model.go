package local

type ResourceSpec struct {
	Version int               `yaml:"version"`
	Name    string            `yaml:"name"`
	Type    string            `yaml:"type"`
	Spec    interface{}       `yaml:"spec"`
	Labels  map[string]string `yaml:"labels"`
}
