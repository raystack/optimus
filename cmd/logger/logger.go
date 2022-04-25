package logger

import (
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"

	"github.com/odpf/optimus/config"
)

type plainFormatter int

func (p *plainFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if len(entry.Data) > 0 {
		var data string
		for key, val := range entry.Data {
			data += fmt.Sprintf("%s: %v ", key, val)
		}
		return []byte(fmt.Sprintf("%s %s\n", entry.Message, data)), nil
	}
	return []byte(fmt.Sprintf("%s\n", entry.Message)), nil
}

// NewDefaultLogger initialzes plain logger
func NewDefaultLogger() log.Logger {
	return log.NewLogrus(
		log.LogrusWithLevel(config.LogLevelInfo.String()),
		log.LogrusWithFormatter(new(plainFormatter)),
	)
}

// NewClientLogger initializes client logger based on log configuration
func NewClientLogger(logConfig config.LogConfig) log.Logger {
	if logConfig.Level == "" {
		return NewDefaultLogger()
	}

	return log.NewLogrus(
		log.LogrusWithLevel(logConfig.Level.String()),
		log.LogrusWithFormatter(new(plainFormatter)),
	)
}
