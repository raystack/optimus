package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/optimus/extension/factory"
	"github.com/odpf/optimus/extension/model"
)

type installResource struct {
	client   model.Client
	manifest *model.Manifest
	metadata *model.Metadata
	release  *model.RepositoryRelease
}

// InstallManager is an extension manager to manage installation process
type InstallManager struct {
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose              bool
	reservedCommandNames []string
}

// NewInstallManager initializes install manager
func NewInstallManager(
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
	reservedCommandNames ...string,
) (*InstallManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &InstallManager{
		manifester:           manifester,
		assetOperator:        assetOperator,
		verbose:              verbose,
		reservedCommandNames: reservedCommandNames,
	}, nil
}

// Install installs extension
func (i *InstallManager) Install(ctx context.Context, remotePath, commandName string) error {
	if err := i.validateInput(remotePath); err != nil {
		return formatError(i.verbose, err, "error validating install input")
	}

	resource, err := i.setupInstallResource(ctx, remotePath, commandName)
	if err != nil {
		return formatError(i.verbose, err, "error setting up installation")
	}

	if err := i.validateResource(resource); err != nil {
		return formatError(i.verbose, err, "error validating metadata for [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	if err := install(ctx, resource.client, i.assetOperator, resource.metadata); err != nil {
		return formatError(i.verbose, err, "error encountered when installing [%s/%s@%s]",
			resource.metadata.OwnerName, resource.metadata.ProjectName, resource.metadata.TagName,
		)
	}

	manifest := i.rebuildManifest(resource)
	if err := i.manifester.Flush(manifest, model.ExtensionDir); err != nil {
		return formatError(i.verbose, err, "error updating manifest")
	}
	return nil
}

func (*InstallManager) rebuildManifest(resource *installResource) *model.Manifest {
	manifest := resource.manifest
	metadata := resource.metadata
	release := resource.release

	var updatedOnOwner bool
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			var updatedOnProject bool
			for _, project := range owner.Projects {
				if project.Name == metadata.ProjectName {
					project.ActiveTagName = metadata.TagName
					project.Releases = append(project.Releases, release)
					updatedOnProject = true
				}
			}
			if !updatedOnProject {
				project := buildProject(metadata, release)
				project.Owner = owner
				owner.Projects = append(owner.Projects, project)
			}
			updatedOnOwner = true
		}
	}
	if !updatedOnOwner {
		project := buildProject(metadata, release)
		owner := buildOwner(metadata, project)
		project.Owner = owner
		manifest.RepositoryOwners = append(manifest.RepositoryOwners, owner)
	}
	manifest.UpdatedAt = time.Now()
	return manifest
}

func (i *InstallManager) validateResource(resource *installResource) error {
	metadata := resource.metadata
	if err := validateCommandNameOnReserved(metadata.CommandName, i.reservedCommandNames); err != nil {
		return err
	}
	manifest := resource.manifest
	if err := i.validateCommandNameOnManifest(manifest, metadata); err != nil {
		return err
	}
	if isInstalled(manifest, metadata) {
		return fmt.Errorf("[%s/%s@%s] is already installed",
			metadata.OwnerName, metadata.ProjectName, metadata.TagName,
		)
	}
	return nil
}

func (*InstallManager) validateCommandNameOnManifest(manifest *model.Manifest, metadata *model.Metadata) error {
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

func (i *InstallManager) setupInstallResource(ctx context.Context, remotePath, commandName string) (*installResource, error) {
	metadata, err := i.extractMetadata(remotePath)
	if err != nil {
		return nil, fmt.Errorf("error extracting metadata: %w", err)
	}
	manifest, err := i.manifester.Load(model.ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	client, err := factory.ClientRegistry.Get(metadata.ProviderName)
	if err != nil {
		return nil, fmt.Errorf("error finding client for provider [%s]: %w", metadata.ProviderName, err)
	}
	release, err := downloadRelease(ctx, client, metadata.CurrentAPIPath, metadata.UpgradeAPIPath)
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

func (*InstallManager) extractMetadata(remotePath string) (*model.Metadata, error) {
	var remoteMetadata *model.Metadata
	for _, parseFn := range factory.ParseRegistry {
		mtdt, err := parseFn(remotePath)
		if errors.Is(err, model.ErrUnrecognizedRemotePath) {
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

func (*InstallManager) validateInput(remotePath string) error {
	if remotePath == "" {
		return model.ErrEmptyRemotePath
	}
	return nil
}
