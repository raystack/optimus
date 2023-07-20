package extension

import (
	"errors"

	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/extension/model"
)

type activateCommand struct {
	logger log.Logger

	project              *model.RepositoryProject
	reservedCommandNames []string
}

func newActivateCommand(logger log.Logger, project *model.RepositoryProject, reservedCommandNames []string) *cobra.Command {
	activate := &activateCommand{
		logger:               logger,
		project:              project,
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

	a.logger.Info("Activating tag [%s] ...", tagName)
	if err := manager.Activate(a.project.CommandName, tagName); err != nil {
		a.logger.Error("... finished with error")
		return err
	}
	a.logger.Info("... finished successfully")
	return nil
}
