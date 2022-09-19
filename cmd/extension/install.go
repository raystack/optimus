package extension

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

type installCommand struct {
	logger log.Logger

	reservedCommandNames []string
}

func newInstallCommand(logger log.Logger, reservedCommandNames []string) *cobra.Command {
	install := &installCommand{
		logger:               logger,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "install REMOTE",
		Short: "install extension based on remote path",
		RunE:  install.RunE,
	}
	cmd.Flags().String("alias", "", "override the command name with the alias")

	return cmd
}

func (i *installCommand) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("one argument for REMOTE path is required")
	}
	remotePath := args[0]

	commandName, _ := cmd.Flags().GetString("alias")
	verbose, _ := cmd.Flags().GetBool("verbose")

	manager, err := getExtensionManager(verbose, i.reservedCommandNames...)
	if err != nil {
		return err
	}

	i.logger.Info("Installing [%s] ...", remotePath)
	ctx := context.Background()
	if err := manager.Install(ctx, remotePath, commandName); err != nil {
		i.logger.Error("... finished with error")
		return err
	}
	i.logger.Info("... finished successfully")
	return nil
}
