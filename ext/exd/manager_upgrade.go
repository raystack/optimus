package exd

// Upgrade upgrades an extension specified by the command name
func (m *Manager) Upgrade(commandName string) error {
	if err := m.validateUpgradeInput(commandName); err != nil {
		return formatError("error validating upgrade: %w", err)
	}

	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return formatError("error loading manifest: %w", err)
	}

	project := m.findProjectByCommandName(manifest, commandName)
	if project == nil {
		return formatError("extension with command name [%s] is not installed", commandName)
	}

	client, err := m.findClientProvider(project.Owner.Provider)
	if err != nil {
		return formatError("error finding client provider [%s]: %w", project.Owner.Provider, err)
	}

	currRelease := m.getLocalReleaseFromProject(project)
	if currRelease == nil {
		return formatError("manifest file is corrupted based on [%s]", commandName)
	}

	upRelease, err := m.getRemoteRelease(client, "", currRelease.UpgradeAPIPath)
	if err != nil {
		return formatError("error getting release for [%s/%s@latest]: %w",
			currRelease.Project.Owner.Name, currRelease.Project.Name, err,
		)
	}

	updateMetadata := &RemoteMetadata{
		ProviderName:   project.Owner.Provider,
		OwnerName:      project.Owner.Name,
		RepoName:       project.Name,
		CommandName:    project.CommandName,
		LocalDirPath:   project.LocalDirPath,
		TagName:        upRelease.TagName,
		CurrentAPIPath: upRelease.CurrentAPIPath,
		UpgradeAPIPath: upRelease.UpgradeAPIPath,
	}
	if m.isAlreadyInstalled(manifest, currRelease.Project.Owner.Name, currRelease.Project.Name, upRelease.TagName) {
		m.updateManifest(manifest, updateMetadata, upRelease)
		if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
			return formatError("error flushing manifest: %w", err)
		}
		return nil
	}

	asset, err := m.downloadAsset(client, upRelease.CurrentAPIPath, upRelease.UpgradeAPIPath)
	if err != nil {
		return formatError("error downloading asset for [%s/%s@%s]: %w",
			currRelease.Project.Owner.Name, currRelease.Project.Name, upRelease.TagName, err,
		)
	}

	if err := m.installAsset(asset, project.LocalDirPath, upRelease.TagName); err != nil {
		return formatError("error installing asset for [%s/%s@%s]: %w",
			upRelease.Project.Owner.Name, upRelease.Project.Name, upRelease.TagName, err,
		)
	}

	m.updateManifest(manifest, updateMetadata, upRelease)
	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError("error flushing manifest: %w", err)
	}
	return nil
}

func (*Manager) getLocalReleaseFromProject(project *RepositoryProject) *RepositoryRelease {
	for _, release := range project.Releases {
		if release.TagName == project.ActiveTagName {
			return release
		}
	}
	return nil
}

func (m *Manager) validateUpgradeInput(commandName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.installer); err != nil {
		return err
	}
	if commandName == "" {
		return ErrEmptyCommandName
	}
	return nil
}
