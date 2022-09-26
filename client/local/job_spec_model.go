package local

type JobSpec struct {
	Version      int               `yaml:"version"`
	Name         string            `yaml:"name"`
	Owner        string            `yaml:"owner"`
	Description  string            `yaml:"description"`
	Schedule     JobSchedule       `yaml:"schedule"`
	Behavior     JobBehavior       `yaml:"behavior"`
	Task         JobTask           `yaml:"task"`
	Asset        map[string]string `yaml:"asset"`
	Labels       map[string]string `yaml:"labels"`
	Dependencies []JobDependency   `yaml:"dependencies`
	Hooks        []JobHook         `yaml:"hooks"`
	Metadata     JobSpecMetadata   `yaml:"metadata"`
}
