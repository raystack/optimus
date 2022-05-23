package extension

import (
	"context"

	"github.com/odpf/optimus/extension/internal"
	"github.com/odpf/optimus/extension/model"
)

// Manager defines the extension management
type Manager struct {
	ctx           context.Context //nolint:containedctx
	httpDoer      model.HTTPDoer
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose              bool
	reservedCommandNames []string
}

// NewManager initializes new manager
func NewManager(
	ctx context.Context,
	httpDoer model.HTTPDoer,
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
	reservedCommandNames ...string,
) (*Manager, error) {
	if ctx == nil {
		return nil, model.ErrNilContext
	}
	if httpDoer == nil {
		return nil, model.ErrNilHTTPDoer
	}
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &Manager{
		ctx:                  ctx,
		httpDoer:             httpDoer,
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
func (m *Manager) Upgrade(commandName string) error {
	manager, err := internal.NewUpgradeManager(
		m.ctx,
		m.httpDoer,
		m.manifester,
		m.assetOperator,
		m.verbose,
	)
	if err != nil {
		return err
	}
	return manager.Upgrade(commandName)
}

// Install runs an extension installation process
func (m *Manager) Install(remotePath, commandName string) error {
	manager, err := internal.NewInstallManager(
		m.ctx,
		m.httpDoer,
		m.manifester,
		m.assetOperator,
		m.verbose,
		m.reservedCommandNames...,
	)
	if err != nil {
		return err
	}
	return manager.Install(remotePath, commandName)
}
