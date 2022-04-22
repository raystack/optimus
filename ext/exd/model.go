package exd

import (
	"context"
	"net/http"
	"time"
)

type Metadata struct {
	ProviderName string `yaml:"provider_name"`
	OwnerName    string `yaml:"owner_name"`
	RepoName     string `yaml:"repo_name"`
	TagName      string `yaml:"tag_name"`

	AssetAPIPath string `yaml:"asset_api_path"`
	AssetDirPath string `yaml:"asset_dir_path"`

	CommandName string `yaml:"command_name"`
}

type Manifest struct {
	UpdatedAt time.Time   `yaml:"updated_at"`
	Metadatas []*Metadata `yaml:"metadatas"`
}

type Parser func(remotePath string) (*Metadata, error)
type Client interface {
	Download(*Metadata) ([]byte, error)
	Install(asset []byte, metadata *Metadata) error
}

// HTTPDoer is an HTTP contract to do an HTTP request
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type NewClient func(ctx context.Context, httpDoer HTTPDoer) (Client, error)

type Manifester interface {
	Load(dirPath string) (*Manifest, error)
	Flush(manifest *Manifest, dirPath string) error
}
