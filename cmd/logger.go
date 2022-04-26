package cmd

import (
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"

	"github.com/odpf/optimus/config"
)

type plainFormatter int

func (*plainFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if len(entry.Data) > 0 {
		var data string
		for key, val := range entry.Data {
			data += fmt.Sprintf("%s: %v ", key, val)
		}
		return []byte(fmt.Sprintf("%s %s\n", entry.Message, data)), nil
	}
	return []byte(fmt.Sprintf("%s\n", entry.Message)), nil
}

func initDefaultLogger() log.Logger {
	return log.NewLogrus(
		log.LogrusWithLevel(config.LogLevelInfo.String()),
		log.LogrusWithFormatter(new(plainFormatter)),
	)
}

func initClientLogger(conf config.LogConfig) log.Logger {
	if conf.Level == "" {
		return initDefaultLogger()
	}

	return log.NewLogrus(
		log.LogrusWithLevel(conf.Level.String()),
		log.LogrusWithFormatter(new(plainFormatter)),
	)
}
