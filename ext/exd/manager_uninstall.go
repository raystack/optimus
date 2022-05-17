package exd

import (
	"fmt"
	"time"
)

type uninstallResource struct {
	manifest *Manifest
	project  *RepositoryProject
	releases []*RepositoryRelease

	dirPath  string
	tagNames []string
}

// Uninstall uninstalls extension based on the command name and the tag
func (m *Manager) Uninstall(commandName, tagName string) error {
	if err := m.validateUninstallInput(commandName, tagName); err != nil {
		return formatError(m.verbose, err, "error validating uninstall input")
	}

	resource, err := m.setupUninstallResource(commandName, tagName)
	if err != nil {
		return formatError(m.verbose, err, "error setting up uninstall")
	}

	if err := m.uninstall(resource); err != nil {
		return formatError(m.verbose, err, "error encountered during uninstallation")
	}

	newManifest := m.rebuildManifstForUninstall(resource)
	if err := m.manifester.Flush(newManifest, ExtensionDir); err != nil {
		return fmt.Errorf("error flushing manifest: %w", err)
	}
	return nil
}

func (m *Manager) rebuildManifstForUninstall(resource *uninstallResource) *Manifest {
	oldManifest := resource.manifest
	oldProject := resource.project
	oldReleasesToRemove := resource.releases

	newOwners := make([]*RepositoryOwner, len(oldManifest.RepositoryOwners))
	for i, o := range oldManifest.RepositoryOwners {
		var newProjects []*RepositoryProject
		for _, p := range o.Projects {
			if p.Name == oldProject.Name {
				if m.isProjectToBeRemoved(oldReleasesToRemove) {
					continue
				}

				newReleases := m.removeReleases(p.Releases, oldReleasesToRemove)
				if len(newReleases) == 0 {
					newReleases = nil
				}
				p.Releases = newReleases
			}
			newProjects = append(newProjects, p)
		}

		if len(newProjects) == 0 {
			newProjects = nil
		}
		o.Projects = newProjects
		newOwners[i] = o
	}

	if len(newOwners) == 0 {
		newOwners = nil
	}
	return &Manifest{
		UpdatedAt:        time.Now(),
		RepositoryOwners: newOwners,
	}
}

func (m *Manager) removeReleases(sourceReleases, releasesToBeRemoved []*RepositoryRelease) []*RepositoryRelease {
	tagNameToReleaseToBeRemoved := make(map[string]*RepositoryRelease)
	for _, r := range releasesToBeRemoved {
		tagNameToReleaseToBeRemoved[r.TagName] = r
	}
	var newReleases []*RepositoryRelease
	for _, r := range sourceReleases {
		if tagNameToReleaseToBeRemoved[r.TagName] == nil {
			newReleases = append(newReleases, r)
		}
	}
	return newReleases
}

func (m *Manager) isProjectToBeRemoved(releases []*RepositoryRelease) bool {
	return len(releases) == 0
}

func (m *Manager) uninstall(resource *uninstallResource) error {
	if err := m.assetOperator.Prepare(resource.dirPath); err != nil {
		return fmt.Errorf("error preparing uninstallation: %w", err)
	}
	if err := m.assetOperator.Uninstall(resource.tagNames...); err != nil {
		return fmt.Errorf("error uninstalling tags: %w", err)
	}
	return nil
}

func (m *Manager) setupUninstallResource(commandName, tagName string) (*uninstallResource, error) {
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := m.findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	releases, err := m.findReleasesFromProject(project, tagName)
	if err != nil {
		return nil, fmt.Errorf("error finding release from project: %w", err)
	}
	tagNames := make([]string, len(releases))
	for i, release := range releases {
		tagNames[i] = release.TagName
	}
	return &uninstallResource{
		manifest: manifest,
		project:  project,
		releases: releases,
		dirPath:  project.LocalDirPath,
		tagNames: tagNames,
	}, nil
}

func (m *Manager) findReleasesFromProject(project *RepositoryProject, tagName string) ([]*RepositoryRelease, error) {
	if tagName == "" {
		return project.Releases, nil
	}
	for _, release := range project.Releases {
		if release.TagName == tagName {
			return []*RepositoryRelease{release}, nil
		}
	}
	return nil, fmt.Errorf("tag [%s] is not installed", tagName)
}

func (m *Manager) validateUninstallInput(commandName, tagName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.assetOperator); err != nil {
		return err
	}
	if commandName == "" {
		return ErrEmptyCommandName
	}
	return nil
}
