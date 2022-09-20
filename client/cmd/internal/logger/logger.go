package logger

import (
	"fmt"
	"io"
	"os"

	"github.com/fatih/color"
	"github.com/odpf/salt/log"
)

type defaultLogger struct {
	writer   io.Writer
	exitFunc func(int)
}

func (d defaultLogger) Debug(msg string, args ...interface{}) {
	c := color.New(color.FgWhite)
	d.write(c, msg, args...)
}

func (d defaultLogger) Info(msg string, args ...interface{}) {
	c := color.New(color.FgWhite)
	d.write(c, msg, args...)
}

func (d defaultLogger) Warn(msg string, args ...interface{}) {
	c := color.New(color.FgYellow)
	d.write(c, msg, args...)
}

func (d defaultLogger) Error(msg string, args ...interface{}) {
	c := color.New(color.FgRed)
	d.write(c, msg, args...)
}

func (d defaultLogger) Fatal(msg string, args ...interface{}) {
	c := color.New(color.FgRed)
	d.write(c, msg, args...)
	d.exitFunc(1)
}

func (defaultLogger) Level() string {
	// this is to adhere to the logger interface
	return ""
}

func (d defaultLogger) Writer() io.Writer {
	return d.writer
}

func (d defaultLogger) write(c *color.Color, msg string, args ...interface{}) {
	plainMessage := fmt.Sprintf(msg, args...)
	c.Fprintln(d.writer, plainMessage)
}

// NewClientLogger initializes client logger
func NewClientLogger() log.Logger {
	return &defaultLogger{
		writer:   os.Stdin,
		exitFunc: os.Exit,
	}
}
