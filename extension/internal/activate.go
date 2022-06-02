package internal

import (
	"fmt"

	"github.com/odpf/optimus/extension/model"
)

// ActivateManager is an extension manater to manage tag activation process
type ActivateManager struct {
	manifester model.Manifester

	verbose bool
}

// NewActivateManager initializes activate manager
func NewActivateManager(manifester model.Manifester, verbose bool) (*ActivateManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	return &ActivateManager{
		manifester: manifester,
		verbose:    verbose,
	}, nil
}

// Activate activates the tag for an extension specified by the command name
func (a *ActivateManager) Activate(commandName, tagName string) error {
	if err := a.validateActivateInput(commandName, tagName); err != nil {
		return FormatError(a.verbose, err, "error validating tag activation input")
	}

	manifest, err := a.manifester.Load(model.ExtensionDir)
	if err != nil {
		return FormatError(a.verbose, err, "error loading manifest")
	}

	project := findProjectByCommandName(manifest, commandName)
	if project == nil {
		return FormatError(a.verbose, err, "command name [%s] is not found", commandName)
	}

	if err := a.activateTagInProject(project, tagName); err != nil {
		return FormatError(a.verbose, err, "error activating tag [%s]", tagName)
	}

	if err := a.manifester.Flush(manifest, model.ExtensionDir); err != nil {
		return FormatError(a.verbose, err, "error applying manifest")
	}
	return nil
}

func (*ActivateManager) activateTagInProject(project *model.RepositoryProject, tagName string) error {
	for _, release := range project.Releases {
		if release.TagName == tagName {
			project.ActiveTagName = tagName
			return nil
		}
	}
	return fmt.Errorf("tag name [%s] is not installed", tagName)
}

func (*ActivateManager) validateActivateInput(commandName, tagName string) error {
	if commandName == "" {
		return model.ErrEmptyCommandName
	}
	if tagName == "" {
		return model.ErrEmptyTagName
	}
	return nil
}
