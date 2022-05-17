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
	LocalDirPath  string               `yaml:"local_dir_path"`
	Releases      []*RepositoryRelease `yaml:"releases"`

	Owner *RepositoryOwner `yaml:"-"`
}

// RepositoryRelease defines the release version of a repository release
type RepositoryRelease struct {
	TagName        string `yaml:"tag_name"`
	CurrentAPIPath string `yaml:"current_api_path"`
	UpgradeAPIPath string `yaml:"upgrade_api_path"`
	// Metadata is additional metadata which might be
	// required. Each provider can define the key
	// and its value according to its own requirements.
	Metadata map[string]interface{} `yaml:"metadata"`
	Assets   []*RepositoryAsset     `yaml:"assets"`

	Project *RepositoryProject `yaml:"-"`
}

// RepositoryAsset defines a specific asset for a release
type RepositoryAsset struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// Metadata defines general metadata for an extension
type Metadata struct {
	ProviderName string
	OwnerName    string
	ProjectName  string
	TagName      string

	CurrentAPIPath string
	UpgradeAPIPath string
	LocalDirPath   string

	CommandName string
}

// Parser is contract that will be defined by each provider
// to parse remote metadata from path
type Parser func(remotePath string) (*Metadata, error)

// Client is a contract that will be defined by each provider
// to execute client-related operation
type Client interface {
	// DownloadRelease downloads a release specified by the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadRelease(string) (*RepositoryRelease, error)
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

// AssetOperator is a contract to operate on extension asset
type AssetOperator interface {
	// Prepare does preparation before any operation.
	// Such preparation can be in the form of, but not limited to,
	// creating local directory. On this example, the argument
	// can be directory path.
	Prepare(string) error
	// Install installs an asset to a targeted path.
	// The first argument should be an asset.
	// The second argument can be any string. If it's related
	// to a plain local installation, then the string
	// can be file name.
	Install([]byte, string) error
	// Uninstall uninstalls asset specified by the paths.
	// The paths can be zero or more strings.
	// These paths can be in the form of, but not limited to,
	// file paths.
	Uninstall(...string) error
}

// Manifester is a contract to operate on manifest file
type Manifester interface {
	Load(dirPath string) (*Manifest, error)
	Flush(manifest *Manifest, dirPath string) error
}
