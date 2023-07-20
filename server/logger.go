package server

import (
	"fmt"
	"io"
	"os"

	"github.com/raystack/salt/log"
)

type defaultLogger struct {
	logger *log.Logrus
}

func (d defaultLogger) Debug(msg string, args ...interface{}) {
	d.logger.Debug(fmt.Sprintf(msg, args...))
}

func (d defaultLogger) Info(msg string, args ...interface{}) {
	d.logger.Info(fmt.Sprintf(msg, args...))
}

func (d defaultLogger) Warn(msg string, args ...interface{}) {
	d.logger.Warn(fmt.Sprintf(msg, args...))
}

func (d defaultLogger) Error(msg string, args ...interface{}) {
	d.logger.Error(fmt.Sprintf(msg, args...))
}

func (d defaultLogger) Fatal(msg string, args ...interface{}) {
	d.logger.Fatal(fmt.Sprintf(msg, args...))
}

func (d defaultLogger) Level() string {
	return d.logger.Level()
}

func (d defaultLogger) Writer() io.Writer {
	return d.logger.Writer()
}

func NewLogger(level string) log.Logger {
	return &defaultLogger{
		logger: log.NewLogrus(
			log.LogrusWithLevel(level),
			log.LogrusWithWriter(os.Stderr),
		),
	}
}
