package logger

import (
	"fmt"
	"io"
	goLog "log"
	"os"

	"github.com/pkg/errors"
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
	log.Formatter = new(logrus.JSONFormatter)
	log.Level = logrus.InfoLevel

	if l, err := logrus.ParseLevel(mode); err != nil {
		fmt.Println(errors.Wrap(err, "using 'info' as default").Error())
	} else {
		log.Level = l
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
