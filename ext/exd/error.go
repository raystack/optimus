package exd

import "errors"

var (
	// ErrUnrecognizedRemotePath is error when remote path is not recognized
	ErrUnrecognizedRemotePath = errors.New("remote path is not recognized")
	// ErrNilMetadata is error when metadata is nil
	ErrNilMetadata = errors.New("metadata is nil")
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
)
