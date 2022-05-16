package exd

import (
	"errors"
	"fmt"
)

var (
	// ErrUnrecognizedRemotePath is error when remote path is not recognized
	ErrUnrecognizedRemotePath = errors.New("remote path is not recognized")
	// ErrNilRemoteMetadata is error when remote metadata is nil
	ErrNilRemoteMetadata = errors.New("remote metadata is nil")
	// ErrNilAsset is error when asset is nil
	ErrNilAsset = errors.New("asset is nil")
	// ErrEmptyProvider is error when provider is empty
	ErrEmptyProvider = errors.New("provider is empty")
	// ErrNilContext is error when context is nil
	ErrNilContext = errors.New("context is nil")
	// ErrNilHTTPDoer is error when http doer is nil
	ErrNilHTTPDoer = errors.New("http doer is nil")
	// ErrNilManifester is error when manifester is nil
	ErrNilManifester = errors.New("manifester is nil")
	// ErrNilInstaller is error when installer is nil
	ErrNilInstaller = errors.New("installer is nil")
	// ErrEmptyRemotePath is error when remote path is empty
	ErrEmptyRemotePath = errors.New("remote path is empty")
	// ErrEmptyAPIPath is error when api path is empty
	ErrEmptyAPIPath = errors.New("api path is empty")
	// ErrEmptyCommandName is error when command name is empty
	ErrEmptyCommandName = errors.New("command name is empty")
	// ErrEmptyTagName is error when tag name is empty
	ErrEmptyTagName = errors.New("tag name is empty")
)

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
	return fmt.Errorf("%s [caused by] %w", message, cause)
}
