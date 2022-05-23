package internal

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/extension/factory"
	"github.com/odpf/optimus/extension/model"
)

func buildOwner(metadata *model.Metadata, project *model.RepositoryProject) *model.RepositoryOwner {
	return &model.RepositoryOwner{
		Name:     metadata.OwnerName,
		Provider: metadata.ProviderName,
		Projects: []*model.RepositoryProject{project},
	}
}

func buildProject(metadata *model.Metadata, release *model.RepositoryRelease) *model.RepositoryProject {
	return &model.RepositoryProject{
		Name:          metadata.ProjectName,
		CommandName:   metadata.CommandName,
		LocalDirPath:  metadata.LocalDirPath,
		ActiveTagName: metadata.TagName,
		Releases:      []*model.RepositoryRelease{release},
	}
}

func install(client model.Client, assetOperator model.AssetOperator, metadata *model.Metadata) error {
	asset, err := downloadAsset(client, metadata.CurrentAPIPath, metadata.UpgradeAPIPath)
	if err != nil {
		return fmt.Errorf("error downloading asset: %w", err)
	}
	if err := installAsset(assetOperator, asset, metadata.LocalDirPath, metadata.TagName); err != nil {
		return fmt.Errorf("error installing asset: %w", err)
	}
	return nil
}

func installAsset(assetOperator model.AssetOperator, asset []byte, localDirPath, tagName string) error {
	if err := assetOperator.Prepare(localDirPath); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	if err := assetOperator.Install(asset, tagName); err != nil {
		return fmt.Errorf("error during installation: %w", err)
	}
	return nil
}

func downloadAsset(client model.Client, currentAPIPath, upgradeAPIPath string) ([]byte, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.DownloadAsset(apiPath)
}

func isInstalled(manifest *model.Manifest, metadata *model.Metadata) bool {
	for _, owner := range manifest.RepositoryOwners {
		if owner.Name == metadata.OwnerName {
			for _, project := range owner.Projects {
				if project.Name == metadata.ProjectName {
					for _, release := range project.Releases {
						if release.TagName == metadata.TagName {
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

func validateCommandNameOnReserved(commandName string, reservedCommandNames []string) error {
	for _, reserved := range reservedCommandNames {
		if reserved == commandName {
			return fmt.Errorf("command [%s] is reserved, try changing it", commandName)
		}
	}
	return nil
}

func downloadRelease(client model.Client, currentAPIPath, upgradeAPIPath string) (*model.RepositoryRelease, error) {
	apiPath := currentAPIPath
	if apiPath == "" {
		apiPath = upgradeAPIPath
	}
	return client.DownloadRelease(apiPath)
}

func findClientProvider(ctx context.Context, httpDoer model.HTTPDoer, provider string) (model.Client, error) {
	newClient, err := factory.NewClientRegistry.Get(provider)
	if err != nil {
		return nil, fmt.Errorf("error getting client initializer: %w", err)
	}
	return newClient(ctx, httpDoer)
}

func findProjectByCommandName(manifest *model.Manifest, commandName string) *model.RepositoryProject {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			if project.CommandName == commandName {
				return project
			}
		}
	}
	return nil
}
