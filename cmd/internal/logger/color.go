package logger

import (
	"github.com/muesli/termenv"
	"github.com/odpf/salt/term"
)

var (
	cs             = term.NewColorScheme()
	ColoredSuccess = func(s string, a ...interface{}) string {
		return cs.Greenf(s, a...)
	}

	tp          = termenv.EnvColorProfile()
	ColorYellow = tp.Color("#DBAB79")
	ColorRed    = tp.Color("#E88388")
)
