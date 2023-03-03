package writer

import (
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type LogLevel int

const (
	LogLevelTrace LogLevel = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelFatal
)

type LogWriter interface {
	Write(LogLevel, string) error
}

func newLogStatusProto(lvl LogLevel, msg string) *pb.Log {
	var logLevel pb.Level
	switch lvl {
	case LogLevelTrace:
		logLevel = pb.Level_LEVEL_TRACE
	case LogLevelDebug:
		logLevel = pb.Level_LEVEL_DEBUG
	case LogLevelInfo:
		logLevel = pb.Level_LEVEL_INFO
	case LogLevelWarning:
		logLevel = pb.Level_LEVEL_WARNING
	case LogLevelError:
		logLevel = pb.Level_LEVEL_ERROR
	case LogLevelFatal:
		logLevel = pb.Level_LEVEL_FATAL
	}
	return &pb.Log{
		Level:   logLevel,
		Message: msg,
	}
}
