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
	metadata, err := m.extractMetadata(remotePath)
	if err != nil {
		return formatError("error extracting metadata for: %w", err)
	}
	client, err := m.findClientProvider(metadata.ProviderName)
	if err != nil {
		return formatError("error finding client provider [%s]: %w", metadata.ProviderName, err)
	}
	release, err := client.GetRelease(metadata.AssetAPIPath)
	if err != nil {
		return formatError("error getting release for [%s/%s@%s]: %w",
			metadata.OwnerName, metadata.RepoName, metadata.TagName, err,
		)
	}
	metadata.TagName = release.Name
	if commandName != "" {
		metadata.CommandName = commandName
	}
	alreadyInstalled := m.isAlreadyInstalled(manifest, metadata)
	if alreadyInstalled {
		return formatError("[%s/%s@%s] is already installed", metadata.OwnerName, metadata.RepoName, metadata.TagName)
	}
	asset, err := client.DownloadAsset(metadata.AssetAPIPath)
	if err != nil {
		return formatError("error downloading asset for [%s/%s@%s]: %w",
			metadata.OwnerName, metadata.RepoName, metadata.TagName, err,
		)
	}
	if err := m.installer.Prepare(metadata); err != nil {
		return formatError("error preparing installation for [%s/%s@%s]: %w",
			metadata.OwnerName, metadata.RepoName, metadata.TagName, err,
		)
	}
	if err := m.installer.Install(asset, metadata); err != nil {
		return formatError("error installing asset for [%s/%s@%s]: %w",
			metadata.OwnerName, metadata.RepoName, metadata.TagName, err,
		)
	}
	if updated := m.updateExistingProjectInManifest(manifest, metadata, release); !updated {
		m.addNewProjectToManifest(manifest, metadata, release)
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

func (*Manager) addNewProjectToManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) {
	manifest.RepositoryOwners = append(manifest.RepositoryOwners, &RepositoryOwner{
		Name:     metadata.OwnerName,
		Provider: metadata.ProviderName,
		Projects: []*RepositoryProject{
			{
				Name:          metadata.RepoName,
				CommandName:   metadata.CommandName,
				ActiveTagName: metadata.TagName,
				AssetAPIPath:  metadata.AssetAPIPath,
				AssetDirPath:  metadata.AssetDirPath,
				Releases: map[string]*RepositoryRelease{
					release.Name: release,
				},
			},
		},
	})
}

func (*Manager) updateExistingProjectInManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.RepoName {
					project.ActiveTagName = metadata.TagName
					project.AssetAPIPath = metadata.AssetAPIPath
					project.AssetDirPath = metadata.AssetDirPath
					project.CommandName = metadata.CommandName
					project.Releases[metadata.TagName] = release
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

func (*Manager) isAlreadyInstalled(manifest *Manifest, metadata *Metadata) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.RepoName {
					for tagName := range project.Releases {
						if tagName == metadata.TagName {
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

func (*Manager) extractMetadata(remotePath string) (*Metadata, error) {
	var metadata *Metadata
	for _, parseFn := range ParseRegistry {
		mtdt, err := parseFn(remotePath)
		if errors.Is(err, ErrUnrecognizedRemotePath) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("errors parsing [%s]: %w", remotePath, err)
		}
		if mtdt != nil {
			metadata = mtdt
			break
		}
	}
	if metadata == nil {
		return nil, fmt.Errorf("[%s] is not recognized", remotePath)
	}
	return metadata, nil
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
