package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/raystack/optimus/client/extension/factory"
	"github.com/raystack/optimus/client/extension/model"
)

type upgradeResource struct {
	client         model.Client
	manifest       *model.Manifest
	metadata       *model.Metadata
	currentRelease *model.RepositoryRelease
	upgradeRelease *model.RepositoryRelease
}

// UpgradeManager is an extension manager to manage upgrade process
type UpgradeManager struct {
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose bool
}

// NewUpgradeManager initializes upgrade manager
func NewUpgradeManager(
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
) (*UpgradeManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &UpgradeManager{
		manifester:    manifester,
		assetOperator: assetOperator,
		verbose:       verbose,
	}, nil
}

// Upgrade upgrades extension specified by the command name
func (u *UpgradeManager) Upgrade(ctx context.Context, commandName string) error {
	if err := u.validateInput(commandName); err != nil {
		return FormatError(u.verbose, err, "error validating upgrade input")
	}

	resource, err := u.setupResource(ctx, commandName)
	if err != nil {
		return FormatError(u.verbose, err, "error setting up upgrade")
	}

	if isInstalled(resource.manifest, resource.metadata) {
		manifest := u.rebuildManifest(resource)
		if err := u.manifester.Flush(manifest, model.ExtensionDir); err != nil {
			return FormatError(u.verbose, err, "error updating manifest")
		}
		return nil
	}

	if err := install(ctx, resource.client, u.assetOperator, resource.metadata); err != nil {
		return FormatError(u.verbose, err, "error encountered during installing [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	manifest := u.rebuildManifest(resource)
	if err := u.manifester.Flush(manifest, model.ExtensionDir); err != nil {
		return FormatError(u.verbose, err, "error updating manifest")
	}
	return nil
}

func (u *UpgradeManager) rebuildManifest(resource *upgradeResource) *model.Manifest {
	manifest := resource.manifest
	metadata := resource.metadata

	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			u.upgradeOwnerWithResource(owner, resource)
			break
		}
	}
	manifest.UpdatedAt = time.Now()
	return manifest
}

func (u *UpgradeManager) upgradeOwnerWithResource(owner *model.RepositoryOwner, resource *upgradeResource) {
	metadata := resource.metadata
	upgradeRelease := resource.upgradeRelease

	for _, project := range owner.Projects {
		if project.Name == metadata.ProjectName {
			u.upgradeProjectWithRelease(project, upgradeRelease)
			break
		}
	}
}

func (*UpgradeManager) upgradeProjectWithRelease(project *model.RepositoryProject, upgradeRelease *model.RepositoryRelease) {
	var isReleaseFound bool
	for _, release := range project.Releases {
		if release.TagName == upgradeRelease.TagName {
			isReleaseFound = true
			break
		}
	}
	if !isReleaseFound {
		project.Releases = append(project.Releases, upgradeRelease)
	}
	project.ActiveTagName = upgradeRelease.TagName
}

func (u *UpgradeManager) setupResource(ctx context.Context, commandName string) (*upgradeResource, error) {
	manifest, err := u.manifester.Load(model.ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	client, err := factory.ClientRegistry.Get(project.Owner.Provider)
	if err != nil {
		return nil, fmt.Errorf("error finding client for provider [%s]: %w", project.Owner.Provider, err)
	}
	currentRelease := u.getCurrentRelease(project)
	if currentRelease == nil {
		return nil, fmt.Errorf("manifest file is corrupted based on [%s]", commandName)
	}
	upgradeRelease, err := downloadRelease(ctx, client, "", currentRelease.UpgradeAPIPath)
	if err != nil {
		return nil, fmt.Errorf("error downloading release for [%s/%s@latest]: %w",
			project.Owner.Name, project.Name, err,
		)
	}
	return &upgradeResource{
		client:   client,
		manifest: manifest,
		metadata: &model.Metadata{
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

func (*UpgradeManager) getCurrentRelease(project *model.RepositoryProject) *model.RepositoryRelease {
	for _, release := range project.Releases {
		if release.TagName == project.ActiveTagName {
			return release
		}
	}
	return nil
}

func (*UpgradeManager) validateInput(commandName string) error {
	if commandName == "" {
		return model.ErrEmptyCommandName
	}
	return nil
}
