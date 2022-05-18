package extension

import (
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

type uninstallCommand struct {
	logger log.Logger

	commandName          string
	reservedCommandNames []string
}

func newUninstallCommand(logger log.Logger, commandName string, reservedCommandNames []string) *cobra.Command {
	uninstall := &uninstallCommand{
		logger:               logger,
		commandName:          commandName,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "uninstall is a sub command to allow user to uninstall a specified tag of an extension",
		RunE:  uninstall.RunE,
	}
	cmd.Flags().StringP("tag", "t", "", "if empty, then the specified extension will be removed entirely")
	return cmd
}

func (r *uninstallCommand) RunE(cmd *cobra.Command, _ []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")
	tagName, _ := cmd.Flags().GetString("tag")

	manager, err := getExtensionManager(verbose, r.reservedCommandNames...)
	if err != nil {
		return err
	}

	if tagName == "" {
		r.logger.Info(fmt.Sprintf("Uninstalling [%s] ...", r.commandName))
	} else {
		r.logger.Info(fmt.Sprintf("Uninstalling [%s@%s] ...", r.commandName, tagName))
	}
	if err := manager.Uninstall(r.commandName, tagName); err != nil {
		r.logger.Error("... finished with error")
		return err
	}
	r.logger.Info("... finished successfully")
	return nil
}
