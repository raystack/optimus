package writer

import (
	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type saltLogger struct {
	l log.Logger
}

func NewLogWriter(l log.Logger) LogWriter {
	return &saltLogger{
		l: l,
	}
}

func (l *saltLogger) Write(level LogLevel, message string) error {
	switch level {
	case LogLevelTrace:
		l.l.Debug(message)
	case LogLevelDebug:
		l.l.Debug(message)
	case LogLevelInfo:
		l.l.Info(message)
	case LogLevelWarning:
		l.l.Warn(message)
	case LogLevelError:
		l.l.Error(message)
	case LogLevelFatal:
		l.l.Fatal(message)
	}
	return nil
}

type BufferedLogger struct {
	Messages []*pb.Log
}

func (b *BufferedLogger) Write(level LogLevel, message string) error {
	b.Messages = append(b.Messages, newLogStatusProto(level, message))
	return nil
}
