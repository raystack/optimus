package extension

import (
	"errors"
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

type renameCommand struct {
	logger log.Logger

	commandName          string
	reservedCommandNames []string
}

func newRenameCommand(logger log.Logger, commandName string, reservedCommandNames []string) *cobra.Command {
	rename := &renameCommand{
		logger:               logger,
		commandName:          commandName,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "rename NEW-NAME",
		Short: "rename is a sub command to allow user to rename an extension command",
		RunE:  rename.RunE,
	}
	return cmd
}

func (r *renameCommand) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("one argument should be specified")
	}
	targetCommandName := args[0]
	verbose, _ := cmd.Flags().GetBool("verbose")

	manager, err := getExtensionManager(verbose, r.reservedCommandNames...)
	if err != nil {
		return err
	}

	r.logger.Info(fmt.Sprintf("Ranaming command [%s] to [%s] ...", r.commandName, targetCommandName))
	if err := manager.Rename(r.commandName, targetCommandName); err != nil {
		r.logger.Error("... finished with error")
		return err
	}
	r.logger.Info("... finished successfully")
	return nil
}
