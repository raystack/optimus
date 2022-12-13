package config

import "os"

const (
	ServerName = "optimus"
	ClientName = "optimus-cli"
)

var (
	// overridden by the build system
	BuildVersion = "v0.4.5-next"
	BuildCommit  = ""
	BuildDate    = ""
)

// AppName returns the name used as identifier in telemetry
func AppName() string {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		return ServerName
	}
	return ClientName
}
