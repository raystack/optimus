package config

import "strconv"

// Contains shared config for server and client (project)

// Version implement fmt.Stringer
type Version int

type LogConfig struct {
	Level  string `mapstructure:"level" default:"info"` // log level - debug, info, warning, error, fatal
	Format string `mapstructure:"format"`               // format strategy - plain, json
}

func (v Version) String() string {
	return strconv.Itoa(int(v))
}
