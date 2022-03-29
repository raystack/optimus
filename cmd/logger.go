package cmd

import (
	"fmt"
	"os"

	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"

	"github.com/odpf/optimus/config"
)

type loggerType int

const (
	jsonLoggerType loggerType = iota
	plainLoggerType
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

func initLogger(t loggerType, conf config.LogConfig) log.Logger {
	if conf.Level == "" {
		conf.Level = config.LogLevelInfo
	}

	switch t {
	case jsonLoggerType:
		return log.NewLogrus(
			log.LogrusWithLevel(conf.Level.String()),
			log.LogrusWithWriter(os.Stderr),
		)
	case plainLoggerType:
		return log.NewLogrus(
			log.LogrusWithLevel(conf.Level.String()),
			log.LogrusWithFormatter(new(plainFormatter)),
		)
	}

	return nil
}
