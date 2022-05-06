package exd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"
)

// ExtensionDir is directory path where to store the extensions
const ExtensionDir = ".optimus/extensions"

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

// Install installs extension based on the remote path
func (m *Manager) Install(remotePath, commandName string) error {
	if err := m.validateInput(remotePath); err != nil {
		return formatError("error validating installation for [%s]: %w", remotePath, err)
	}
	dirPath := m.getExtensionDirPath()
	manifest, err := m.manifester.Load(dirPath)
	if err != nil {
		return formatError("error loading manifest: %w", err)
	}
	remoteMetadata, err := m.extractMetadata(remotePath)
	if err != nil {
		return formatError("error extracting remote metadata for: %w", err)
	}
	client, err := m.findClientProvider(remoteMetadata.ProviderName)
	if err != nil {
		return formatError("error finding client provider [%s]: %w", remoteMetadata.ProviderName, err)
	}
	release, err := client.GetRelease(remoteMetadata.AssetAPIPath)
	if err != nil {
		return formatError("error getting release for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	remoteMetadata.TagName = release.TagName
	if commandName != "" {
		remoteMetadata.CommandName = commandName
	}
	alreadyInstalled := m.isAlreadyInstalled(manifest, remoteMetadata)
	if alreadyInstalled {
		return formatError("[%s/%s@%s] is already installed", remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName)
	}
	asset, err := client.DownloadAsset(remoteMetadata.AssetAPIPath)
	if err != nil {
		return formatError("error downloading asset for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	if err := m.installer.Prepare(remoteMetadata); err != nil {
		return formatError("error preparing installation for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	if err := m.installer.Install(asset, remoteMetadata); err != nil {
		return formatError("error installing asset for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	if updated := m.updateExistingProjectInManifest(manifest, remoteMetadata, release); !updated {
		m.addNewProjectToManifest(manifest, remoteMetadata, release)
	}
	if err := m.applyManifest(manifest, dirPath); err != nil {
		return formatError("error applying manifest: %w", err)
	}
	return nil
}

func (m *Manager) applyManifest(manifest *Manifest, dirPath string) error {
	manifest.UpdatedAt = time.Now()
	return m.manifester.Flush(manifest, dirPath)
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
				AssetAPIPath:  remoteMetadata.AssetAPIPath,
				AssetDirPath:  remoteMetadata.AssetDirPath,
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
					project.AssetAPIPath = remoteMetadata.AssetAPIPath
					project.AssetDirPath = remoteMetadata.AssetDirPath
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

func (m *Manager) findClientProvider(provider string) (Client, error) {
	newClient, err := NewClientRegistry.Get(provider)
	if err != nil {
		return nil, fmt.Errorf("error getting new client: %w", err)
	}
	return newClient(m.ctx, m.httpDoer)
}

func (*Manager) isAlreadyInstalled(manifest *Manifest, remoteMetadata *RemoteMetadata) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == remoteMetadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == remoteMetadata.RepoName {
					for _, release := range project.Releases {
						if release.TagName == remoteMetadata.TagName {
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

func (*Manager) extractMetadata(remotePath string) (*RemoteMetadata, error) {
	var remoteMetadata *RemoteMetadata
	for _, parseFn := range ParseRegistry {
		mtdt, err := parseFn(remotePath)
		if errors.Is(err, ErrUnrecognizedRemotePath) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("errors parsing [%s]: %w", remotePath, err)
		}
		if mtdt != nil {
			remoteMetadata = mtdt
			break
		}
	}
	if remoteMetadata == nil {
		return nil, fmt.Errorf("[%s] is not recognized", remotePath)
	}
	return remoteMetadata, nil
}

func (*Manager) getExtensionDirPath() string {
	userHomeDir, _ := os.UserHomeDir()
	return path.Join(userHomeDir, ExtensionDir)
}

func (m *Manager) validateInput(remotePath string) error {
	if remotePath == "" {
		return ErrEmptyRemotePath
	}
	return validate(m.ctx, m.httpDoer, m.manifester, m.installer)
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
