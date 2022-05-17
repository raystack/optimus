package exd

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"
)

// ExtensionDir is directory path where to store the extensions
var ExtensionDir string

// Manager defines the extension management
type Manager struct {
	ctx        context.Context //nolint:containedctx
	httpDoer   HTTPDoer
	manifester Manifester
	installer  Installer

	verbose bool
}

// NewManager initializes new manager
func NewManager(
	ctx context.Context,
	httpDoer HTTPDoer,
	manifester Manifester,
	installer Installer,
	verbose bool,
) (*Manager, error) {
	if err := validate(ctx, httpDoer, manifester, installer); err != nil {
		return nil, fmt.Errorf("error validating parameter: %w", err)
	}
	return &Manager{
		ctx:        ctx,
		httpDoer:   httpDoer,
		manifester: manifester,
		installer:  installer,
		verbose:    verbose,
	}, nil
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

func (m *Manager) installAsset(asset []byte, dirPath, fileName string) error {
	if err := m.installer.Prepare(dirPath); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	if err := m.installer.Install(asset, dirPath, fileName); err != nil {
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

func (m *Manager) updateManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) error {
	if updated := m.updateExistingProjectInManifest(manifest, metadata, release); !updated {
		m.addNewProjectToManifest(manifest, metadata, release)
	}
	manifest.UpdatedAt = time.Now()
	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return fmt.Errorf("error flushing manifest: %w", err)
	}
	return nil
}

func (*Manager) addNewProjectToManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) {
	manifest.RepositoryOwners = append(manifest.RepositoryOwners, &RepositoryOwner{
		Name:     metadata.OwnerName,
		Provider: metadata.ProviderName,
		Projects: []*RepositoryProject{
			{
				Name:          metadata.ProjectName,
				CommandName:   metadata.CommandName,
				ActiveTagName: metadata.TagName,
				LocalDirPath:  metadata.LocalDirPath,
				Releases:      []*RepositoryRelease{release},
			},
		},
	})
}

func (*Manager) updateExistingProjectInManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.ProjectName {
					project.ActiveTagName = metadata.TagName
					project.LocalDirPath = metadata.LocalDirPath
					project.CommandName = metadata.CommandName
					for _, r := range project.Releases {
						if r.TagName == project.ActiveTagName {
							return true
						}
					}
					project.Releases = append(project.Releases, release)
					return true
				}
			}
			break
		}
	}
	return false
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

func validate(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) error {
	if ctx == nil {
		return ErrNilContext
	}
	if httpDoer == nil {
		return ErrNilHTTPDoer
	}
	if manifester == nil {
		return ErrNilManifester
	}
	if installer == nil {
		return ErrNilInstaller
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
