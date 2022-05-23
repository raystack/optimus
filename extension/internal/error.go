package internal

import "fmt"

func formatError(verbose bool, cause error, format string, a ...interface{}) error {
	if verbose {
		return formatVerboseErr(cause, format, a...)
	}
	return formatSimpleErr(format, a...)
}

func formatSimpleErr(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

func formatVerboseErr(cause error, format string, a ...interface{}) error {
	message := fmt.Sprintf(format, a...)
	return fmt.Errorf("%s (caused by) %w", message, cause)
}
