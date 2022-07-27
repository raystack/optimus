package writer

import "github.com/odpf/salt/log"

type logrus struct {
	l log.Logrus
}

func NewLogrusWriter(l log.Logrus) LogWriter {
	return &logrus{
		l: l,
	}
}

func (l *logrus) Write(level LogLevel, message string) error {
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
