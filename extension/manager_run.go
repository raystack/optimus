package extension

import (
	"fmt"
)

type runResource struct {
	localDirPath string
	tagName      string

	args []string
}

// Run executes an installed extension
func (m *Manager) Run(commandName string, args ...string) error {
	if err := m.validateRunInput(commandName, args...); err != nil {
		return formatError(m.verbose, err, "error validating run input")
	}

	resource, err := m.setupRunResource(commandName, args...)
	if err != nil {
		return formatError(m.verbose, err, "error setting up run")
	}

	if err := m.run(resource); err != nil {
		return formatError(m.verbose, err, "error running extension")
	}
	return nil
}

func (m *Manager) run(resource *runResource) error {
	if err := m.assetOperator.Prepare(resource.localDirPath); err != nil {
		return fmt.Errorf("error preparing run: %w", err)
	}
	if err := m.assetOperator.Run(resource.tagName, resource.args...); err != nil {
		return fmt.Errorf("error during running: %w", err)
	}
	return nil
}

func (m *Manager) setupRunResource(commandName string, args ...string) (*runResource, error) {
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := m.findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	return &runResource{
		tagName:      project.ActiveTagName,
		localDirPath: project.LocalDirPath,
		args:         args,
	}, nil
}

func (m *Manager) validateRunInput(commandName string, _ ...string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.assetOperator); err != nil {
		return err
	}
	if commandName == "" {
		return ErrEmptyCommandName
	}
	return nil
}
