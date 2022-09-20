package model

import (
	"context"
)

// Parser is contract that will be defined by each provider
// to parse remote metadata from path
type Parser func(remotePath string) (*Metadata, error)

// Client is a contract that will be defined by each provider
// to execute client-related operation
type Client interface {
	// DownloadRelease downloads a release specified by the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadRelease(context.Context, string) (*RepositoryRelease, error)
	// DownloadAsset downloads asset based on the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadAsset(context.Context, string) ([]byte, error)
}
