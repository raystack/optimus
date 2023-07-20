package extension

import (
	"errors"

	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/extension/model"
)

type renameCommand struct {
	logger log.Logger

	project              *model.RepositoryProject
	reservedCommandNames []string
}

func newRenameCommand(logger log.Logger, project *model.RepositoryProject, reservedCommandNames []string) *cobra.Command {
	rename := &renameCommand{
		logger:               logger,
		project:              project,
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

	r.logger.Info("Ranaming command [%s] to [%s] ...", r.project.CommandName, targetCommandName)
	if err := manager.Rename(r.project.CommandName, targetCommandName); err != nil {
		r.logger.Error("... finished with error")
		return err
	}
	r.logger.Info("... finished successfully")
	return nil
}
