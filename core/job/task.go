package job

type Task struct {
	name   string
	config *Config
}

func (t Task) Name() string {
	return t.name
}

func (t Task) Config() *Config {
	return t.config
}

func NewTask(name string, config *Config) *Task {
	return &Task{name: name, config: config}
}
