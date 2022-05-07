package exd

import "fmt"

// Rename renames an existing command name into a targeted command name
func (m *Manager) Rename(sourceCommandName, targetCommandName string) error {
	if err := m.validateRenameCommandInput(sourceCommandName, targetCommandName); err != nil {
		return formatError("error validating rename command: %w", err)
	}
	if sourceCommandName == targetCommandName {
		return nil
	}

	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return formatError("error loading manifest: %w", err)
	}

	sourceProject := m.getProjectByCommandName(manifest, sourceCommandName)
	if sourceProject == nil {
		return fmt.Errorf("source command name [%s] is not found", sourceCommandName)
	}

	targetProject := m.getProjectByCommandName(manifest, targetCommandName)
	if targetProject != nil {
		return fmt.Errorf("target command name [%s] is already used", targetCommandName)
	}

	sourceProject.CommandName = targetCommandName
	if err := m.manifester.Flush(manifest, ExtensionDir); err != nil {
		return formatError("error flushing manifest: %w", err)
	}
	return nil
}

func (m *Manager) validateRenameCommandInput(sourceCommandName, targetCommandName string) error {
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.installer); err != nil {
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
