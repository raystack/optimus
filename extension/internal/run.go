package internal

import (
	"fmt"

	"github.com/odpf/optimus/extension/model"
)

type runResource struct {
	localDirPath string
	tagName      string

	args []string
}

// RunManager is an extension manager to manage run operation
type RunManager struct {
	manifester    model.Manifester
	assetOperator model.AssetOperator

	verbose bool
}

// NewRunManager initializes run manager
func NewRunManager(
	manifester model.Manifester,
	assetOperator model.AssetOperator,
	verbose bool,
) (*RunManager, error) {
	if manifester == nil {
		return nil, model.ErrNilManifester
	}
	if assetOperator == nil {
		return nil, model.ErrNilAssetOperator
	}
	return &RunManager{
		manifester:    manifester,
		assetOperator: assetOperator,
		verbose:       verbose,
	}, nil
}

// Run executes an installed extension
func (r *RunManager) Run(commandName string, args ...string) error {
	if err := r.validateInput(commandName, args...); err != nil {
		return FormatError(r.verbose, err, "error validating run input")
	}

	resource, err := r.setupResource(commandName, args...)
	if err != nil {
		return FormatError(r.verbose, err, "error setting up run")
	}

	if err := r.run(resource); err != nil {
		return FormatError(r.verbose, err, "error running extension")
	}
	return nil
}

func (r *RunManager) run(resource *runResource) error {
	if err := r.assetOperator.Prepare(resource.localDirPath); err != nil {
		return fmt.Errorf("error preparing run: %w", err)
	}
	if err := r.assetOperator.Run(resource.tagName, resource.args...); err != nil {
		return fmt.Errorf("error during running: %w", err)
	}
	return nil
}

func (r *RunManager) setupResource(commandName string, args ...string) (*runResource, error) {
	manifest, err := r.manifester.Load(model.ExtensionDir)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest: %w", err)
	}
	project := findProjectByCommandName(manifest, commandName)
	if project == nil {
		return nil, fmt.Errorf("extension with command name [%s] is not installed", commandName)
	}
	return &runResource{
		tagName:      project.ActiveTagName,
		localDirPath: project.LocalDirPath,
		args:         args,
	}, nil
}

func (*RunManager) validateInput(commandName string, _ ...string) error {
	if commandName == "" {
		return model.ErrEmptyCommandName
	}
	return nil
}
