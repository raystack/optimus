package exd

import (
	"context"
	"net/http"
	"time"
)

// Manifest describes extensions' information
type Manifest struct {
	UpdatedAt        time.Time          `yaml:"updated_at"`
	RepositoryOwners []*RepositoryOwner `yaml:"repository_owners"`
}

// RepositoryOwner represents the owner of an extension repository
type RepositoryOwner struct {
	Name     string               `yaml:"name"`
	Provider string               `yaml:"provider"`
	Projects []*RepositoryProject `yaml:"projects"`
}

// RepositoryProject represents the repository or place
// where the extension projects resides
type RepositoryProject struct {
	Name          string               `yaml:"name"`
	CommandName   string               `yaml:"command_name"`
	ActiveTagName string               `yaml:"active_tag_name"`
	AssetAPIPath  string               `yaml:"asset_api_path"`
	AssetDirPath  string               `yaml:"asset_dir_path"`
	Releases      []*RepositoryRelease `yaml:"releases"`
}

// RepositoryRelease defines the release version of a repository release
type RepositoryRelease struct {
	TagName string `yaml:"tag_name"`
	// Metadata is additional metadata which might be
	// required. Each provider can define the key
	// and its value according to its own requirements.
	Metadata map[string]interface{} `yaml:"metadata"`
	Assets   []*RepositoryAsset     `yaml:"assets"`
}

// RepositoryAsset defines a specific asset for a release
type RepositoryAsset struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

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

// Parser is contract that will be defined by each provider
// to parse metadata
type Parser func(remotePath string) (*Metadata, error)

// Client is a contract that will be defined by each provider
// to execute client-related operation
type Client interface {
	// GetRelease gets a release specified by the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	GetRelease(string) (*RepositoryRelease, error)
	// DownloadAsset downloads asset based on the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadAsset(string) ([]byte, error)
}

// HTTPDoer is an HTTP contract to do an HTTP request
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewClient is a contract that will be defined by each provider
// to initialize client related to that provider
type NewClient func(ctx context.Context, httpDoer HTTPDoer) (Client, error)

// Installer is a contract to install extension based on
// metadata and its asset
type Installer interface {
	Prepare(metadata *Metadata) error
	Install(asset []byte, metadata *Metadata) error
}

// Manifester is a contract to operate on manifest file
type Manifester interface {
	Load(dirPath string) (*Manifest, error)
	Flush(manifest *Manifest, dirPath string) error
}
