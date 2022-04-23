package exd

import (
	"context"
	"net/http"
	"time"
)

// Metadata defines metadata for an extension
type Metadata struct {
	ProviderName string `yaml:"provider_name"`
	OwnerName    string `yaml:"owner_name"`
	RepoName     string `yaml:"repo_name"`
	TagName      string `yaml:"tag_name"`

	AssetAPIPath string `yaml:"asset_api_path"`
	AssetDirPath string `yaml:"asset_dir_path"`

	CommandName string `yaml:"command_name"`
}

// Manifest describes extensions' information
type Manifest struct {
	UpdatedAt time.Time   `yaml:"updated_at"`
	Metadatas []*Metadata `yaml:"metadatas"`
}

// Parser is contract that will be defined by each provider
// to parse metadata
type Parser func(remotePath string) (*Metadata, error)

// Client is a contract that will be defined by each provider
// to execute client-related operation
type Client interface {
	Download(*Metadata) ([]byte, error)
}

// HTTPDoer is an HTTP contract to do an HTTP request
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewClient is a contract that will be defined by each provider
// to initialize client related to that provider
type NewClient func(ctx context.Context, httpDoer HTTPDoer) (Client, error)

// Manifester is a contract to operate on manifest file
type Manifester interface {
	Load(dirPath string) (*Manifest, error)
	Flush(manifest *Manifest, dirPath string) error
}

// Installer is a contract to install extension based on
// its asset and metadata
type Installer interface {
	Prepare(*Metadata) error
	Install([]byte, *Metadata) error
}
