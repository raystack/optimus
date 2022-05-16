package exd

import (
	"errors"
	"fmt"
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

	release, err := m.getRemoteRelease(client, remoteMetadata.CurrentAPIPath, remoteMetadata.UpgradeAPIPath)
	if err != nil {
		return formatError("error getting release for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}
	m.updateRemoteMetadata(remoteMetadata, release, commandName)

	if err := m.validateRemoteMetadataToManifest(remoteMetadata, manifest); err != nil {
		return formatError("error remote metadata on manifest: %w", err)
	}

	if m.isAlreadyInstalled(manifest, remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName) {
		return formatError("[%s/%s@%s] is already installed",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName,
		)
	}

	asset, err := m.downloadAsset(client, remoteMetadata.CurrentAPIPath, remoteMetadata.UpgradeAPIPath)
	if err != nil {
		return formatError("error downloading asset for [%s/%s@%s]: %w",
			remoteMetadata.OwnerName, remoteMetadata.RepoName, remoteMetadata.TagName, err,
		)
	}

	if err := m.installAsset(asset, remoteMetadata.LocalDirPath, remoteMetadata.TagName); err != nil {
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

func (*Manager) updateRemoteMetadata(remoteMetadata *RemoteMetadata, release *RepositoryRelease, commandName string) {
	if commandName != "" {
		remoteMetadata.CommandName = commandName
	}
	remoteMetadata.TagName = release.TagName
	remoteMetadata.CurrentAPIPath = release.CurrentAPIPath
	remoteMetadata.UpgradeAPIPath = release.UpgradeAPIPath
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
