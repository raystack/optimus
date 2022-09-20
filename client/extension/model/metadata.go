package model

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
