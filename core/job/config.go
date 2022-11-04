package job

type Config struct {
	config map[string]string
}

func (c Config) Config() map[string]string {
	return c.config
}

func NewConfig(config map[string]string) *Config {
	return &Config{config: config}
}
