package extension

import (
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

type upgradeCommand struct {
	logger log.Logger

	commandName          string
	reservedCommandNames []string
}

func newUpgradeCommand(logger log.Logger, commandName string, reservedCommandNames []string) *cobra.Command {
	upgrade := &upgradeCommand{
		logger:               logger,
		commandName:          commandName,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "upgrade is a sub command to allow user to upgrade an extension command",
		RunE:  upgrade.RunE,
	}
	return cmd
}

func (u *upgradeCommand) RunE(cmd *cobra.Command, _ []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")

	manager, err := getExtensionManager(verbose, u.reservedCommandNames...)
	if err != nil {
		return err
	}

	u.logger.Info(fmt.Sprintf("Upgrading [%s] ...", u.commandName))
	if err := manager.Upgrade(u.commandName); err != nil {
		u.logger.Error("... finished with error")
		return err
	}
	u.logger.Info("... finished successfully")
	return nil
}
