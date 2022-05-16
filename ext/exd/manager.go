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
}

// NewManager initializes new manager
func NewManager(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) (*Manager, error) {
	if err := validate(ctx, httpDoer, manifester, installer); err != nil {
		return nil, fmt.Errorf("error validating parameter: %w", err)
	}
	return &Manager{
		ctx:        ctx,
		httpDoer:   httpDoer,
		manifester: manifester,
		installer:  installer,
	}, nil
}

func (m *Manager) updateManifest(manifest *Manifest, remoteMetadata *RemoteMetadata, release *RepositoryRelease) {
	if updated := m.updateExistingProjectInManifest(manifest, remoteMetadata, release); !updated {
		m.addNewProjectToManifest(manifest, remoteMetadata, release)
	}
	manifest.UpdatedAt = time.Now()
}

func (*Manager) addNewProjectToManifest(manifest *Manifest, remoteMetadata *RemoteMetadata, release *RepositoryRelease) {
	manifest.RepositoryOwners = append(manifest.RepositoryOwners, &RepositoryOwner{
		Name:     remoteMetadata.OwnerName,
		Provider: remoteMetadata.ProviderName,
		Projects: []*RepositoryProject{
			{
				Name:          remoteMetadata.RepoName,
				CommandName:   remoteMetadata.CommandName,
				ActiveTagName: remoteMetadata.TagName,
				LocalDirPath:  remoteMetadata.LocalDirPath,
				Releases:      []*RepositoryRelease{release},
			},
		},
	})
}

func (*Manager) updateExistingProjectInManifest(manifest *Manifest, remoteMetadata *RemoteMetadata, release *RepositoryRelease) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == remoteMetadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == remoteMetadata.RepoName {
					project.ActiveTagName = remoteMetadata.TagName
					project.LocalDirPath = remoteMetadata.LocalDirPath
					project.CommandName = remoteMetadata.CommandName
					project.Releases = append(project.Releases, release)
					return true
				}
			}
			break
		}
	}
	return false
}

func (*Manager) findProjectByCommandName(manifest *Manifest, commandName string) *RepositoryProject {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			if project.CommandName == commandName {
				return project
			}
		}
	}
	return nil
}

func (*Manager) isAlreadyInstalled(manifest *Manifest, ownerName, projectName, tagName string) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == ownerName {
			for _, project := range owner.Projects {
				if project.Name == projectName {
					for _, release := range project.Releases {
						if release.TagName == tagName {
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

func (*Manager) getRemoteRelease(client Client, currentAPIPath, upgradeAPIPath string) (*RepositoryRelease, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.GetRelease(apiPath)
}

func (m *Manager) findClientProvider(provider string) (Client, error) {
	newClient, err := NewClientRegistry.Get(provider)
	if err != nil {
		return nil, err
	}
	return newClient(m.ctx, m.httpDoer)
}

func (*Manager) downloadAsset(client Client, currentAPIPath, upgradeAPIPath string) ([]byte, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.DownloadAsset(apiPath)
}

func (m *Manager) installAsset(asset []byte, dirPath, fileName string) error {
	if err := m.installer.Prepare(dirPath); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	return m.installer.Install(asset, dirPath, fileName)
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
