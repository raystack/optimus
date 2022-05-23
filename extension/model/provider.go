package model

import (
	"context"
	"net/http"
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

// HTTPDoer is an HTTP contract to do an HTTP request
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewClient is a contract that will be defined by each provider
// to initialize client related to that provider
type NewClient func(httpDoer HTTPDoer) (Client, error)
