package exd

import "fmt"

// Activate activates the tag for an extension specified by the command name
func (m *Manager) Activate(commandName, tagName string) error {
	if err := m.validateActivateInput(commandName, tagName); err != nil {
		return formatError(m.verbose, err, "error validating tag activation input")
	}

	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return formatError(m.verbose, err, "error loading manifest")
	}

	project := m.findProjectByCommandName(manifest, commandName)
	if project == nil {
		return formatError(m.verbose, err, "command name [%s] is not found", commandName)
	}

	if err := m.activateTagInProject(project, tagName); err != nil {
		return formatError(m.verbose, err, "error activating tag [%s]", tagName)
	}

	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError(m.verbose, err, "error applying manifest")
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

func (m *Manager) validateActivateInput(commandName, tagName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.assetOperator); err != nil {
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
