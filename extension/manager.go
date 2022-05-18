package extension

import (
	"context"
	"fmt"
	"os"
	"path"
)

// ExtensionDir is directory path where to store the extensions
var ExtensionDir string

// Manager defines the extension management
type Manager struct {
	ctx           context.Context //nolint:containedctx
	httpDoer      HTTPDoer
	manifester    Manifester
	assetOperator AssetOperator

	verbose              bool
	reservedCommandNames []string
}

// NewManager initializes new manager
func NewManager(
	ctx context.Context,
	httpDoer HTTPDoer,
	manifester Manifester,
	assetOperator AssetOperator,
	verbose bool,
	reservedCommandNames ...string,
) (*Manager, error) {
	if err := validate(ctx, httpDoer, manifester, assetOperator); err != nil {
		return nil, fmt.Errorf("error validating parameter: %w", err)
	}
	return &Manager{
		ctx:                  ctx,
		httpDoer:             httpDoer,
		manifester:           manifester,
		assetOperator:        assetOperator,
		verbose:              verbose,
		reservedCommandNames: reservedCommandNames,
	}, nil
}

func (*Manager) buildOwner(metadata *Metadata, project *RepositoryProject) *RepositoryOwner {
	return &RepositoryOwner{
		Name:     metadata.OwnerName,
		Provider: metadata.ProviderName,
		Projects: []*RepositoryProject{project},
	}
}

func (*Manager) buildProject(metadata *Metadata, release *RepositoryRelease) *RepositoryProject {
	return &RepositoryProject{
		Name:          metadata.ProjectName,
		CommandName:   metadata.CommandName,
		LocalDirPath:  metadata.LocalDirPath,
		ActiveTagName: metadata.TagName,
		Releases:      []*RepositoryRelease{release},
	}
}

func (m *Manager) install(client Client, metadata *Metadata) error {
	asset, err := m.downloadAsset(client, metadata.CurrentAPIPath, metadata.UpgradeAPIPath)
	if err != nil {
		return fmt.Errorf("error downloading asset: %w", err)
	}
	if err := m.installAsset(asset, metadata.LocalDirPath, metadata.TagName); err != nil {
		return fmt.Errorf("error installing asset: %w", err)
	}
	return nil
}

func (m *Manager) installAsset(asset []byte, localDirPath, tagName string) error {
	if err := m.assetOperator.Prepare(localDirPath); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	if err := m.assetOperator.Install(asset, tagName); err != nil {
		return fmt.Errorf("error during installation: %w", err)
	}
	return nil
}

func (*Manager) downloadAsset(client Client, currentAPIPath, upgradeAPIPath string) ([]byte, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.DownloadAsset(apiPath)
}

func (*Manager) isInstalled(manifest *Manifest, metadata *Metadata) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.ProjectName {
					for _, release := range project.Releases {
						if release.TagName == metadata.TagName {
							return true
						}
					}
					return false
				}
			}
			return false
		}
	}
	return false
}

func (*Manager) downloadRelease(client Client, currentAPIPath, upgradeAPIPath string) (*RepositoryRelease, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.DownloadRelease(apiPath)
}

func (m *Manager) findClientProvider(provider string) (Client, error) {
	newClient, err := NewClientRegistry.Get(provider)
	if err != nil {
		return nil, fmt.Errorf("error getting client initializer: %w", err)
	}
	return newClient(m.ctx, m.httpDoer)
}

func validate(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, assetOperator AssetOperator) error {
	if ctx == nil {
		return ErrNilContext
	}
	if httpDoer == nil {
		return ErrNilHTTPDoer
	}
	if manifester == nil {
		return ErrNilManifester
	}
	if assetOperator == nil {
		return ErrNilAssetOperator
	}
	return nil
}

func init() { //nolint:gochecknoinits
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	ExtensionDir = path.Join(userHomeDir, ".optimus/extensions")
}
