package extension

import (
	"context"

	"github.com/goto/optimus/client/extension/internal"
	"github.com/goto/optimus/client/extension/model"
)

// Manager defines the extension management
type Manager struct {
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose              bool
	reservedCommandNames []string
}

// NewManager initializes new manager
func NewManager(
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
	reservedCommandNames ...string,
) (*Manager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &Manager{
		manifester:           manifester,
		assetOperator:        assetOperator,
		verbose:              verbose,
		reservedCommandNames: reservedCommandNames,
	}, nil
}

// Activate runs an extension activation process
func (m *Manager) Activate(commandName, tagName string) error {
	manager, err := internal.NewActivateManager(
		m.manifester,
		m.verbose,
	)
	if err != nil {
		return err
	}
	return manager.Activate(commandName, tagName)
}

// Rename runs an extension renaming process
func (m *Manager) Rename(sourceCommandName, targetCommandName string) error {
	manager, err := internal.NewRenameManager(
		m.manifester,
		m.verbose,
		m.reservedCommandNames...,
	)
	if err != nil {
		return err
	}
	return manager.Rename(sourceCommandName, targetCommandName)
}

// Run runs an extension execution process
func (m *Manager) Run(commandName string, args ...string) error {
	manager, err := internal.NewRunManager(
		m.manifester,
		m.assetOperator,
		m.verbose,
	)
	if err != nil {
		return err
	}
	return manager.Run(commandName, args...)
}

// Uninstall runs an extension uninstall process
func (m *Manager) Uninstall(commandName, tagName string) error {
	manager, err := internal.NewUninstallManager(
		m.manifester,
		m.assetOperator,
		m.verbose,
	)
	if err != nil {
		return err
	}
	return manager.Uninstall(commandName, tagName)
}

// Upgrade runs an extension upgrade process
func (m *Manager) Upgrade(ctx context.Context, commandName string) error {
	manager, err := internal.NewUpgradeManager(
		m.manifester,
		m.assetOperator,
		m.verbose,
	)
	if err != nil {
		return err
	}
	return manager.Upgrade(ctx, commandName)
}

// Install runs an extension installation process
func (m *Manager) Install(ctx context.Context, remotePath, commandName string) error {
	manager, err := internal.NewInstallManager(
		m.manifester,
		m.assetOperator,
		m.verbose,
		m.reservedCommandNames...,
	)
	if err != nil {
		return err
	}
	return manager.Install(ctx, remotePath, commandName)
}
