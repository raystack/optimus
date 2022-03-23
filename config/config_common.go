package config

import "strconv"

// Contains shared config for server and client (project)

// Config is just an alias for interface{}
type Config interface{}

// Version implement fmt.Stringer
type Version int

type LogLevel string

const (
	LogLevelDebug   LogLevel = "DEBUG"
	LogLevelInfo             = "INFO"
	LogLevelWarning          = "INFO"
	LogLevelError            = "ERROR"
	LogLevelFatal            = "FATAL"
)

type LogConfig struct {
	Level  string `mapstructure:"level" default:"info"` // log level - debug, info, warning, error, fatal
	Format string `mapstructure:"format"`               // format strategy - plain, json
}

func (v Version) String() string {
	return strconv.Itoa(int(v))
}
