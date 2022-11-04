package job

type Hook struct {
	name   string
	config *Config
}

func (h Hook) Name() string {
	return h.name
}

func (h Hook) Config() *Config {
	return h.config
}

func NewHook(name string, config *Config) *Hook {
	return &Hook{name: name, config: config}
}
