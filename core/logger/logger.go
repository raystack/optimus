package logger

import (
	"fmt"
	"io"
	goLog "log"
	"os"

	"github.com/sirupsen/logrus"
)

var log *logrus.Logger
var entry *logrus.Entry

const (
	DEBUG   = "DEBUG"
	INFO    = "INFO"
	WARNING = "WARNING"
	ERROR   = "ERROR"
	FATAL   = "FATAL"
)

func Init(mode string) {
	InitWithWriter(mode, os.Stderr)
}

func InitWithWriter(mode string, writer io.Writer) {
	if log != nil {
		return
	}
	log = logrus.New()
	log.Out = writer
	//log.Formatter = new(logrus.JSONFormatter)

	switch mode {
	case DEBUG:
		log.Level = logrus.DebugLevel
	case INFO:
		log.Level = logrus.InfoLevel
	case WARNING:
		log.Level = logrus.WarnLevel
	case ERROR:
		log.Level = logrus.ErrorLevel
	case FATAL:
		log.Level = logrus.FatalLevel
	default:
		fmt.Println("invalid log level. using DEBUG as default")
		log.Level = logrus.DebugLevel
	}

	entry = logrus.NewEntry(log)
	D("logger initialized with log level ", log.Level)
}

func filterFieldsMap(args ...interface{}) (logrus.Fields, []interface{}) {
	if log == nil {
		goLog.Panicf("logger is not initialized, use logger.Init(logger.INFO)")
	}
	if fieldsMap, ok := args[len(args)-1].(map[string]interface{}); ok {
		return logrus.Fields{"payload": fieldsMap}, args[:(len(args) - 1)]
	}
	return nil, args
}

func I(args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Info(args...)
}

func If(format string, args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Infof(format, args...)
}

func Df(format string, args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Debugf(format, args...)
}

func D(args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Debug(args...)
}

func W(args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Warn(args...)
}

func E(args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Error(args...)
}

func F(args ...interface{}) {
	fieldMap, args := filterFieldsMap(args...)
	entry.WithFields(fieldMap).Fatal(args...)
}

func Logger(k, v string) *logrus.Entry {
	return log.WithField(k, v)
}

func Level() logrus.Level {
	return log.Level
}
