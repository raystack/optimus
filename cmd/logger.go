package cmd

import (
	"io"
)

type logger interface {
	Print(...interface{})
	Println(...interface{})
	Printf(string, ...interface{})
	Writer() io.Writer
}
