package cmd

import (
	"fmt"
	"os"

	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"
)

// conf -> client / server / ,, , ,,
// logger -> json / plain / xml / ...
// func -> 4

type loggerType int

const (
	jsonLogger loggerType = iota
	plainLogger
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
	case jsonLogger:
		return log.NewLogrus(
			log.LogrusWithLevel(conf.Level),
			log.LogrusWithWriter(os.Stderr),
		)
	case plainLogger:
		return log.NewLogrus(
			log.LogrusWithLevel(conf.Level),
			log.LogrusWithFormatter(new(plainFormatter)),
		)
	}

	return nil
}
