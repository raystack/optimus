package logger

import "fmt"

var (
	// ColoredError format message with color for error
	ColoredError = fmt.Sprintf
	// ColoredNotice format message with color for notification
	ColoredNotice = fmt.Sprintf
	// ColoredSuccess format message with color for success
	ColoredSuccess = fmt.Sprintf
)
