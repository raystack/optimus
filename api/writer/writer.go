package writer

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
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
		logLevel = pb.Level_Trace
	case LogLevelDebug:
		logLevel = pb.Level_Debug
	case LogLevelInfo:
		logLevel = pb.Level_Info
	case LogLevelWarning:
		logLevel = pb.Level_Warning
	case LogLevelError:
		logLevel = pb.Level_Error
	case LogLevelFatal:
		logLevel = pb.Level_Fatal
	}
	return &pb.Log{
		Level:   logLevel,
		Message: msg,
	}
}
