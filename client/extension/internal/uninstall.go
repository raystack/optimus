package internal

import (
	"fmt"
	"time"

	"github.com/raystack/optimus/client/extension/model"
)

type uninstallResource struct {
	manifest *model.Manifest
	project  *model.RepositoryProject
	releases []*model.RepositoryRelease

	localDirPath string
	tagNames     []string
}

// UninstallManager is an extension manager to manage uninstallation process
type UninstallManager struct {
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose bool
}

// NewUninstallManager initializes uninstall manager
func NewUninstallManager(
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
) (*UninstallManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &UninstallManager{
		manifester:    manifester,
		assetOperator: assetOperator,
		verbose:       verbose,
	}, nil
}

// Uninstall uninstalls extension based on the command name and the tag
func (u *UninstallManager) Uninstall(commandName, tagName string) error {
	if err := u.validateInput(commandName, tagName); err != nil {
		return FormatError(u.verbose, err, "error validating uninstall input")
	}

	resource, err := u.setupResource(commandName, tagName)
	if err != nil {
		return FormatError(u.verbose, err, "error setting up uninstall")
	}

	if err := u.uninstall(resource); err != nil {
		return FormatError(u.verbose, err, "error encountered during uninstallation")
	}

	newManifest := u.rebuildManifest(resource)
	if err := u.manifester.Flush(newManifest, model.ExtensionDir); err != nil {
		return fmt.Errorf("error flushing manifest: %w", err)
	}
	return nil
}

func (u *UninstallManager) rebuildManifest(resource *uninstallResource) *model.Manifest {
	oldManifest := resource.manifest
	oldProject := resource.project
	oldReleasesToRemove := resource.releases
	isOldProjectToBeRemoved := len(oldReleasesToRemove) == 0

	var newOwners []*model.RepositoryOwner
	for _, owner := range oldManifest.RepositoryOwners {
		var newProjects []*model.RepositoryProject
		for _, project := range owner.Projects {
			var newReleases []*model.RepositoryRelease
			if project.Name == oldProject.Name {
				if isOldProjectToBeRemoved {
					continue
				}
				newReleases = u.removeReleases(project.Releases, oldReleasesToRemove)
			}
			if len(newReleases) > 0 {
				u.setReleasesForProject(project, newReleases)
				newProjects = append(newProjects, project)
			}
		}
		owner.Projects = newProjects
		if len(owner.Projects) > 0 {
			newOwners = append(newOwners, owner)
		}
	}

	if len(newOwners) == 0 {
		newOwners = nil
	}
	return &model.Manifest{
		UpdatedAt:        time.Now(),
		RepositoryOwners: newOwners,
	}
}

func (*UninstallManager) setReleasesForProject(project *model.RepositoryProject, releases []*model.RepositoryRelease) {
	var activeTagInRelease bool
	for _, r := range releases {
		if r.TagName == project.ActiveTagName {
			activeTagInRelease = true
			break
		}
	}
	if !activeTagInRelease {
		project.ActiveTagName = releases[0].TagName
	}
	project.Releases = releases
}

func (*UninstallManager) removeReleases(sourceReleases, releasesToBeRemoved []*model.RepositoryRelease) []*model.RepositoryRelease {
	tagNameToReleaseToBeRemoved := make(map[string]*model.RepositoryRelease)
	for _, r := range releasesToBeRemoved {
		tagNameToReleaseToBeRemoved[r.TagName] = r
	}
	var newReleases []*model.RepositoryRelease
	for _, r := range sourceReleases {
		if tagNameToReleaseToBeRemoved[r.TagName] == nil {
			newReleases = append(newReleases, r)
		}
	}
	return newReleases
}

func (u *UninstallManager) uninstall(resource *uninstallResource) error {
	if err := u.assetOperator.Prepare(resource.localDirPath); err != nil {
		return fmt.Errorf("error preparing uninstallation: %w", err)
	}
	if err := u.assetOperator.Uninstall(resource.tagNames...); err != nil {
		return fmt.Errorf("error uninstalling tags: %w", err)
	}
	return nil
}

func (u *UninstallManager) setupResource(commandName, tagName string) (*uninstallResource, error) {
	manifest, err := u.manifester.Load(model.ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	releases, err := u.findReleasesFromProject(project, tagName)
	if err != nil {
		return nil, fmt.Errorf("error finding release from project: %w", err)
	}
	tagNames := make([]string, len(releases))
	for i, release := range releases {
		tagNames[i] = release.TagName
	}
	return &uninstallResource{
		manifest:     manifest,
		project:      project,
		releases:     releases,
		localDirPath: project.LocalDirPath,
		tagNames:     tagNames,
	}, nil
}

func (*UninstallManager) findReleasesFromProject(project *model.RepositoryProject, tagName string) ([]*model.RepositoryRelease, error) {
	if tagName == "" {
		return project.Releases, nil
	}
	for _, release := range project.Releases {
		if release.TagName == tagName {
			return []*model.RepositoryRelease{release}, nil
		}
	}
	return nil, fmt.Errorf("tag [%s] is not installed", tagName)
}

func (*UninstallManager) validateInput(commandName, _ string) error {
	if commandName == "" {
		return model.ErrEmptyCommandName
	}
	return nil
}
