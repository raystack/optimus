package exd

import (
	"errors"
	"fmt"
	"time"
)

// Install installs extension based on the remote path
func (m *Manager) Install(remotePath, commandName string) error {
	if err := m.validateInstallInput(remotePath, commandName); err != nil {
		return formatError("error validating install: %w", err)
	}

	manifest, err := m.manifester.Load(ExtensionDir)
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

	release, err := m.getRelease(client, remoteMetadata)
	if err != nil {
		return formatError("error getting release for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	m.updateRemoteMetadata(remoteMetadata, release, commandName)

	if err := m.validateRemoteMetadataToManifest(remoteMetadata, manifest); err != nil {
		return formatError("error remote metadata on manifest: %w", err)
	}

	if m.isAlreadyInstalled(manifest, remoteMetadata) {
		return formatError("[%s/%s@%s] is already installed",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName,
		)
	}

	asset, err := m.downloadAsset(client, remoteMetadata)
	if err != nil {
		return formatError("error downloading asset for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}

	if err := m.installAsset(asset, remoteMetadata); err != nil {
		return formatError("error installing asset for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}

	m.updateManifest(manifest, remoteMetadata, release)
	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError("error flushing manifest: %w", err)
	}
	return nil
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

func (m *Manager) installAsset(asset []byte, remoteMetadata *RemoteMetadata) error {
	if err := m.installer.Prepare(remoteMetadata); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	return m.installer.Install(asset, remoteMetadata)
}

func (*Manager) validateRemoteMetadataToManifest(remoteMetadata *RemoteMetadata, manifest *Manifest) error {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			if project.CommandName == remoteMetadata.CommandName {
				if owner.Name == remoteMetadata.OwnerName && project.Name == remoteMetadata.RepoName {
					return nil
				}
				return fmt.Errorf("command [%s] is already used by [%s/%s@%s]",
					remoteMetadata.CommandName, owner.Name, project.Name, project.ActiveTagName,
				)
			}
		}
	}
	return nil
}

func (*Manager) downloadAsset(client Client, remoteMetadata *RemoteMetadata) ([]byte, error) {
	apiPath := remoteMetadata.CurrentAPIPath
	if apiPath == "" {
		apiPath = remoteMetadata.UpgradeAPIPath
	}
	return client.DownloadAsset(apiPath)
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

func (*Manager) updateRemoteMetadata(remoteMetadata *RemoteMetadata, release *RepositoryRelease, commandName string) {
	if commandName != "" {
		remoteMetadata.CommandName = commandName
	}
	remoteMetadata.TagName = release.TagName
	remoteMetadata.CurrentAPIPath = release.CurrentAPIPath
	remoteMetadata.UpgradeAPIPath = release.UpgradeAPIPath
}

func (*Manager) getRelease(client Client, remoteMetadata *RemoteMetadata) (*RepositoryRelease, error) {
	apiPath := remoteMetadata.CurrentAPIPath
	if apiPath == "" {
		apiPath = remoteMetadata.UpgradeAPIPath
	}
	return client.GetRelease(apiPath)
}

func (m *Manager) findClientProvider(provider string) (Client, error) {
	newClient, err := NewClientRegistry.Get(provider)
	if err != nil {
		return nil, fmt.Errorf("error getting new client: %w", err)
	}
	return newClient(m.ctx, m.httpDoer)
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

func (m *Manager) validateInstallInput(remotePath, _ string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.installer); err != nil {
		return err
	}
	if remotePath == "" {
		return ErrEmptyRemotePath
	}
	return nil
}
