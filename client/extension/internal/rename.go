package internal

import (
	"fmt"

	"github.com/goto/optimus/client/extension/model"
)

// RenameManager is an extension manater to manage command rename process
type RenameManager struct {
	manifester model.Manifester

	reservedCommandNames []string
	verbose              bool
}

// NewRenameManager initializes rename manager
func NewRenameManager(
	manifester model.Manifester,
	verbose bool,
	reservedCommandNames ...string,
) (*RenameManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	return &RenameManager{
		manifester:           manifester,
		reservedCommandNames: reservedCommandNames,
		verbose:              verbose,
	}, nil
}

// Rename renames an existing command name into a targeted command name
func (r *RenameManager) Rename(sourceCommandName, targetCommandName string) error {
	if err := r.validateInput(sourceCommandName, targetCommandName); err != nil {
		return FormatError(r.verbose, err, "error validating rename command input")
	}
	if sourceCommandName == targetCommandName {
		return nil
	}

	manifest, err := r.manifester.Load(model.ExtensionDir)
	if err != nil {
		return FormatError(r.verbose, err, "error loading manifest")
	}

	sourceProject := findProjectByCommandName(manifest, sourceCommandName)
	if sourceProject == nil {
		return fmt.Errorf("source command name [%s] is not found", sourceCommandName)
	}

	targetProject := findProjectByCommandName(manifest, targetCommandName)
	if targetProject != nil {
		return fmt.Errorf("target command name [%s] is already used by [%s/%s@%s]",
			targetCommandName, targetProject.Owner.Name, targetProject.Name, targetProject.ActiveTagName,
		)
	}

	sourceProject.CommandName = targetCommandName
	if err := r.manifester.Flush(manifest, model.ExtensionDir); err != nil {
		return FormatError(r.verbose, err, "error applying manifest")
	}
	return nil
}

func (r *RenameManager) validateInput(sourceCommandName, targetCommandName string) error {
	if sourceCommandName == "" {
		return fmt.Errorf("source command: %w", model.ErrEmptyCommandName)
	}
	if targetCommandName == "" {
		return fmt.Errorf("target command: %w", model.ErrEmptyCommandName)
	}
	return validateCommandNameOnReserved(targetCommandName, r.reservedCommandNames)
}
