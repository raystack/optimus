package model

import "time"

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

// Manifester is a contract to operate on manifest file
type Manifester interface {
	Load(dirPath string) (*Manifest, error)
	Flush(manifest *Manifest, dirPath string) error
}
