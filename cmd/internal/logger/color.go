package logger

import (
	"fmt"

	"github.com/muesli/termenv"
	"github.com/odpf/salt/term"
)

var (
	// ColoredSuccess format message with color for success
	ColoredSuccess = fmt.Sprintf

	tp          = termenv.EnvColorProfile()
	ColorYellow = tp.Color("#FFAF00")
	ColorWhite  = tp.Color("#FFFFFF")
	ColorRed    = tp.Color("#D70000")
)

func InitializeColor() {
	cs := term.NewColorScheme()
	ColoredSuccess = func(s string, a ...interface{}) string {
		return cs.Greenf(s, a...)
	}
}
