package extension

import (
	"errors"
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

type activateCommand struct {
	logger log.Logger

	commandName          string
	reservedCommandNames []string
}

func newActivateCommand(logger log.Logger, commandName string, reservedCommandNames []string) *cobra.Command {
	activate := &activateCommand{
		logger:               logger,
		commandName:          commandName,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "activate TAG",
		Short: "activate is a sub command to allow user to activate an installed tag",
		RunE:  activate.RunE,
	}
	return cmd
}

func (a *activateCommand) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("one argument for TAG should be specified")
	}
	tagName := args[0]
	verbose, _ := cmd.Flags().GetBool("verbose")

	manager, err := getExtensionManager(verbose, a.reservedCommandNames...)
	if err != nil {
		return err
	}

	a.logger.Info(fmt.Sprintf("Activating tag [%s] ...", tagName))
	if err := manager.Activate(a.commandName, tagName); err != nil {
		a.logger.Error("... finished with error")
		return err
	}
	a.logger.Info("... finished successfully")
	return nil
}
