package exd

import "fmt"

// Rename renames an existing command name into a targeted command name
func (m *Manager) Rename(sourceCommandName, targetCommandName string) error {
	if err := m.validateRenameInput(sourceCommandName, targetCommandName); err != nil {
		return formatError(m.verbose, err, "error validating rename command input")
	}
	if sourceCommandName == targetCommandName {
		return nil
	}

	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return formatError(m.verbose, err, "error loading manifest")
	}

	sourceProject := m.findProjectByCommandName(manifest, sourceCommandName)
	if sourceProject == nil {
		return fmt.Errorf("source command name [%s] is not found", sourceCommandName)
	}

	targetProject := m.findProjectByCommandName(manifest, targetCommandName)
	if targetProject != nil {
		return fmt.Errorf("target command name [%s] is already used by [%s/%s@%s]",
			targetCommandName, targetProject.Owner.Name, targetProject.Name, targetProject.ActiveTagName,
		)
	}

	sourceProject.CommandName = targetCommandName
	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError(m.verbose, err, "error applying manifest")
	}
	return nil
}

func (m *Manager) validateRenameInput(sourceCommandName, targetCommandName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.assetOperator); err != nil {
		return err
	}
	if sourceCommandName == "" {
		return fmt.Errorf("source command: %w", ErrEmptyCommandName)
	}
	if targetCommandName == "" {
		return fmt.Errorf("target command: %w", ErrEmptyCommandName)
	}
	return nil
}
