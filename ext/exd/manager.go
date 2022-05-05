package exd

import (
	"context"
	"errors"
	"fmt"
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
		return fmt.Errorf("error validating installation: %w", err)
	}
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return fmt.Errorf("error loading manifest: %w", err)
	}
	metadata, err := m.extractMetadata(remotePath)
	if err != nil {
		return fmt.Errorf("error extracting metadata: %w", err)
	}
	client, err := m.findClientProvider(metadata.ProviderName)
	if err != nil {
		return fmt.Errorf("error finding client provider: %w", err)
	}
	release, err := client.GetRelease(metadata.AssetAPIPath)
	if err != nil {
		return fmt.Errorf("error getting release: %w", err)
	}
	metadata.TagName = release.Name
	if commandName != "" {
		metadata.CommandName = commandName
	}
	alreadyInstalled := m.isAlreadyInstalled(manifest, metadata)
	if alreadyInstalled {
		return fmt.Errorf("[%s] is already installed", remotePath)
	}
	asset, err := client.DownloadAsset(metadata.AssetAPIPath)
	if err != nil {
		return fmt.Errorf("error downloading asset: %w", err)
	}
	if err := m.installer.Prepare(metadata); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	if err := m.installer.Install(asset, metadata); err != nil {
		return fmt.Errorf("error installing asset: %w", err)
	}
	return m.updateManifest(manifest, metadata, release)
}

func (m *Manager) updateManifest(manifest *Manifest, metadata *Metadata, release *RepositoryRelease) error {
	var updated bool
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.RepoName {
					project.ActiveTagName = metadata.TagName
					project.AssetAPIPath = metadata.AssetAPIPath
					project.AssetDirPath = metadata.AssetDirPath
					project.CommandName = metadata.CommandName
					project.Releases[metadata.TagName] = release

					updated = true
					break
				}
			}
			break
		}
	}
	if !updated {
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
	manifest.UpdatedAt = time.Now()
	return m.manifester.Flush(manifest, ExtensionDir)
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
				if project.Name == metadata.RepoName &&
					project.ActiveTagName == metadata.TagName {
					return true
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
			return nil, fmt.Errorf("errors parsing remote path: %w", err)
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
