package logger

import (
	"fmt"

	"github.com/muesli/termenv"
	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"

	"github.com/odpf/optimus/config"
)

type colorFormatter int

func (*colorFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var colorcode = ColorWhite
	switch entry.Level {
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		colorcode = ColorRed
	case logrus.WarnLevel:
		colorcode = ColorYellow
	}
	if len(entry.Data) > 0 {
		var data string
		for key, val := range entry.Data {
			data += fmt.Sprintf("%s: %v ", key, val)
		}
		return []byte(termenv.String(fmt.Sprintf("%s %s \n", entry.Message, data)).Foreground(colorcode).String()), nil
	}
	return []byte(termenv.String(fmt.Sprintf("%s\n", entry.Message)).Foreground(colorcode).String()), nil
}

// NewDefaultLogger initialzes plain logger
func NewDefaultLogger() log.Logger {
	return log.NewLogrus(
		log.LogrusWithLevel(config.LogLevelInfo.String()),
		log.LogrusWithFormatter(new(colorFormatter)),
	)
}

// NewClientLogger initializes client logger based on log configuration
func NewClientLogger(logConfig config.LogConfig) log.Logger {
	if logConfig.Level == "" {
		return NewDefaultLogger()
	}

	return log.NewLogrus(
		log.LogrusWithLevel(logConfig.Level.String()),
		log.LogrusWithFormatter(new(colorFormatter)),
	)
}
