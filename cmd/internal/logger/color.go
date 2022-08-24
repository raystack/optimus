package logger

import (
	"fmt"

	"github.com/muesli/termenv"
)

var (
	// ColoredSuccess format message with color for success
	ColoredSuccess = fmt.Sprintf

	tp          = termenv.EnvColorProfile()
	ColorYellow = tp.Color("#FFAF00")
	ColorWhite  = tp.Color("#FFFFFF")
	ColorRed    = tp.Color("#D70000")
)
