package exd

import (
	"errors"
	"fmt"
)

type installResource struct {
	client   Client
	manifest *Manifest
	metadata *Metadata
	release  *RepositoryRelease
}

// Install installs extension based on the remote path
func (m *Manager) Install(remotePath, commandName string) error {
	if err := m.validateInstallInput(remotePath, commandName); err != nil {
		return formatError(m.verbose, err, "error validating install input")
	}

	resource, err := m.setupInstallResource(remotePath, commandName)
	if err != nil {
		return formatError(m.verbose, err, "error setting up installation")
	}

	if err := m.validateInstallResource(resource); err != nil {
		return formatError(m.verbose, err, "error validating metadata for [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	if err := m.install(resource.client, resource.metadata); err != nil {
		return formatError(m.verbose, err, "error encountered when installing [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	if err := m.updateManifest(resource.manifest, resource.metadata, resource.release); err != nil {
		return formatError(m.verbose, err, "error updating manifest")
	}
	return nil
}

func (m *Manager) validateInstallResource(resource *installResource) error {
	manifest := resource.manifest
	metadata := resource.metadata
	if err := m.validateCommandName(manifest, metadata); err != nil {
		return err
	}
	if m.isInstalled(manifest, metadata) {
		return fmt.Errorf("[%s/%s@%s] is already installed",
			metadata.OwnerName, metadata.ProjectName, metadata.TagName,
		)
	}
	return nil
}

func (*Manager) validateCommandName(manifest *Manifest, metadata *Metadata) error {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			if project.CommandName == metadata.CommandName {
				if owner.Name == metadata.OwnerName && project.Name == metadata.ProjectName {
					return nil
				}
				return fmt.Errorf("command [%s] is already used by [%s/%s@%s]",
					metadata.CommandName, owner.Name, project.Name, project.ActiveTagName,
				)
			}
		}
	}
	return nil
}

func (m *Manager) setupInstallResource(remotePath, commandName string) (*installResource, error) {
	metadata, err := m.extractMetadata(remotePath)
	if err != nil {
		return nil, fmt.Errorf("error extracting metadata: %w", err)
	}
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	client, err := m.findClientProvider(metadata.ProviderName)
	if err != nil {
		return nil, fmt.Errorf("error finding client for provider [%s]: %w", metadata.ProviderName, err)
	}
	release, err := m.downloadRelease(client, metadata.CurrentAPIPath, metadata.UpgradeAPIPath)
	if err != nil {
		return nil, fmt.Errorf("error downloading release: %w", err)
	}
	metadata.TagName = release.TagName
	metadata.CurrentAPIPath = release.CurrentAPIPath
	metadata.UpgradeAPIPath = release.UpgradeAPIPath
	if commandName != "" {
		metadata.CommandName = commandName
	}
	return &installResource{
		manifest: manifest,
		client:   client,
		metadata: metadata,
		release:  release,
	}, nil
}

func (*Manager) extractMetadata(remotePath string) (*Metadata, error) {
	var remoteMetadata *Metadata
	for _, parseFn := range ParseRegistry {
		mtdt, err := parseFn(remotePath)
		if errors.Is(err, ErrUnrecognizedRemotePath) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("error parsing remote path [%s]: %w", remotePath, err)
		}
		if mtdt != nil {
			remoteMetadata = mtdt
			break
		}
	}
	if remoteMetadata == nil {
		return nil, fmt.Errorf("remote path [%s] is not recognized", remotePath)
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
