package exd

import "fmt"

type upgradeResource struct {
	client         Client
	manifest       *Manifest
	metadata       *Metadata
	currentRelease *RepositoryRelease
	upgradeRelease *RepositoryRelease
}

// Upgrade upgrades an extension specified by the command name
func (m *Manager) Upgrade(commandName string) error {
	if err := m.validateUpgradeInput(commandName); err != nil {
		return formatError(m.verbose, err, "error validating upgrade input")
	}

	resource, err := m.setupUpgradeResource(commandName)
	if err != nil {
		return formatError(m.verbose, err, "error preparing upgrade")
	}

	if m.isInstalled(resource.manifest, resource.metadata) {
		if err := m.updateManifest(resource.manifest, resource.metadata, resource.upgradeRelease); err != nil {
			return formatError(m.verbose, err, "error updating manifest")
		}
		return nil
	}

	if err := m.install(resource.client, resource.metadata); err != nil {
		return formatError(m.verbose, err, "error encountered during installing [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	if err := m.updateManifest(resource.manifest, resource.metadata, resource.upgradeRelease); err != nil {
		return formatError(m.verbose, err, "error updating manifest")
	}
	return nil
}

func (m *Manager) setupUpgradeResource(commandName string) (*upgradeResource, error) {
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := m.findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	client, err := m.findClientProvider(project.Owner.Provider)
	if err != nil {
		return nil, fmt.Errorf("error finding client for provider [%s]: %w", project.Owner.Provider, err)
	}
	currentRelease := m.getCurrentRelease(project)
	if currentRelease == nil {
		return nil, fmt.Errorf("manifest file is corrupted based on [%s]", commandName)
	}
	upgradeRelease, err := m.downloadRelease(client, "", currentRelease.UpgradeAPIPath)
	if err != nil {
		return nil, fmt.Errorf("error downloading release for [%s/%s@latest]: %w",
			project.Owner.Name, project.Name, err,
		)
	}
	return &upgradeResource{
		client:   client,
		manifest: manifest,
		metadata: &Metadata{
			ProviderName:   project.Owner.Provider,
			OwnerName:      project.Owner.Name,
			ProjectName:    project.Name,
			CommandName:    project.CommandName,
			LocalDirPath:   project.LocalDirPath,
			TagName:        upgradeRelease.TagName,
			CurrentAPIPath: upgradeRelease.CurrentAPIPath,
			UpgradeAPIPath: upgradeRelease.UpgradeAPIPath,
		},
		currentRelease: currentRelease,
		upgradeRelease: upgradeRelease,
	}, nil
}

func (*Manager) getCurrentRelease(project *RepositoryProject) *RepositoryRelease {
	for _, release := range project.Releases {
		if release.TagName == project.ActiveTagName {
			return release
		}
	}
	return nil
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

func (m *Manager) validateUpgradeInput(commandName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.assetOperator); err != nil {
		return err
	}
	if commandName == "" {
		return ErrEmptyCommandName
	}
	return nil
}
