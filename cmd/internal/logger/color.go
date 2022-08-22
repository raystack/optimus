package logger

import (
	"fmt"

	"github.com/muesli/termenv"
)

var (
	// ColoredError format message with color for error
	ColoredError = fmt.Sprintf
	// ColoredNotice format message with color for notification
	ColoredNotice = fmt.Sprintf
	// ColoredSuccess format message with color for success
	ColoredSuccess = fmt.Sprintf

	tp          = termenv.EnvColorProfile()
	ColorYellow = tp.Color("#FFAF00")
	ColorWhite  = tp.Color("#FFFFFF")
	ColorRed    = tp.Color("#D70000")
)
