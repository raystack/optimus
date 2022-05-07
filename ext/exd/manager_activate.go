package exd

import "fmt"

// Activate activates the tag for an extension specified by the command name
func (m *Manager) Activate(commandName, tagName string) error {
	if err := m.validateActivate(commandName, tagName); err != nil {
		return formatError("error validating activate: %w", err)
	}

	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return formatError("error loading manifest: %w", err)
	}

	project := m.getProjectByCommandName(manifest, commandName)
	if project == nil {
		return formatError("command name [%s] is not found", commandName)
	}

	if err := m.activateTagInProject(project, tagName); err != nil {
		return formatError("error updating tag [%s]: %w", tagName, err)
	}

	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError("error flushing manifest: %w", err)
	}
	return nil
}

func (*Manager) activateTagInProject(project *RepositoryProject, tagName string) error {
	for _, release := range project.Releases {
		if release.TagName == tagName {
			project.ActiveTagName = tagName
			return nil
		}
	}
	return fmt.Errorf("tag name [%s] is not installed", tagName)
}

func (*Manager) getProjectByCommandName(manifest *Manifest, commandName string) *RepositoryProject {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			if project.CommandName == commandName {
				return project
			}
		}
	}
	return nil
}

func (m *Manager) validateActivate(commandName, tagName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.installer); err != nil {
		return err
	}
	if commandName == "" {
		return ErrEmptyCommandName
	}
	if tagName == "" {
		return ErrEmptyTagName
	}
	return nil
}
