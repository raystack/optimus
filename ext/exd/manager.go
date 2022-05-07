package exd

import (
	"context"
	"fmt"
	"os"
	"path"
)

// ExtensionDir is directory path where to store the extensions
var ExtensionDir string

// Manager defines the extension management
type Manager struct {
	ctx        context.Context //nolint:containedctx
	httpDoer   HTTPDoer
	manifester Manifester
	installer  Installer
}

// NewManager initializes new manager
func NewManager(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) (*Manager, error) {
	if err := validate(ctx, httpDoer, manifester, installer); err != nil {
		return nil, fmt.Errorf("error validating parameter: %w", err)
	}
	return &Manager{
		ctx:        ctx,
		httpDoer:   httpDoer,
		manifester: manifester,
		installer:  installer,
	}, nil
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

func validate(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) error {
	if ctx == nil {
		return ErrNilContext
	}
	if httpDoer == nil {
		return ErrNilHTTPDoer
	}
	if manifester == nil {
		return ErrNilManifester
	}
	if installer == nil {
		return ErrNilInstaller
	}
	return nil
}

func init() { //nolint:gochecknoinits
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	ExtensionDir = path.Join(userHomeDir, ".optimus/extensions")
}
