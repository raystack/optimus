package extension

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/extension"
)

type installCommand struct {
	logger    log.Logger
	installer extension.Installer

	alias string
}

func newInstallCommand(installer extension.Installer) *cobra.Command {
	install := &installCommand{
		logger:    logger.NewDefaultLogger(),
		installer: installer,
	}

	cmd := &cobra.Command{
		Use:   "install OWNER/REPO",
		Short: "Install an extension",
		RunE:  install.RunE,
	}
	cmd.Flags().StringVarP(&install.alias, "alias", "a", "", "alias to be set for the extension")
	return cmd
}

func (i *installCommand) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("one argument for [owner/repo] is required")
	}
	splitArg := strings.Split(args[0], "/")
	if len(splitArg) != 2 || splitArg[0] == "" || splitArg[1] == "" {
		return errors.New("argument should follow pattern [owner/repo]")
	}

	owner := splitArg[0]
	repo := splitArg[1]

	i.logger.Info(fmt.Sprintf("Installing %s/%s ...", owner, repo))
	ctx := context.Background()
	err := i.installer.Install(ctx, owner, repo, i.alias)
	if err != nil {
		return err
	}
	i.logger.Info("... success")
	return nil
}
