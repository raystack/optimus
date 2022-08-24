package extension

import (
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/survey"
	"github.com/odpf/optimus/extension/model"
)

type uninstallCommand struct {
	logger log.Logger
	survey *survey.ExtensionSurvey

	project              *model.RepositoryProject
	reservedCommandNames []string
}

func newUninstallCommand(logger log.Logger, project *model.RepositoryProject, reservedCommandNames []string) *cobra.Command {
	uninstall := &uninstallCommand{
		logger:               logger,
		survey:               survey.NewExtensionSurvey(),
		project:              project,
		reservedCommandNames: reservedCommandNames,
	}

	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "uninstall is a sub command to allow user to uninstall a specified tag of an extension",
		RunE:  uninstall.RunE,
	}
	cmd.Flags().StringP("tag", "t", "", "if empty, then the specified extension will be removed entirely")
	cmd.Flags().BoolP("yes", "y", false, "if true, then no confirmation will be asked")
	return cmd
}

func (r *uninstallCommand) RunE(cmd *cobra.Command, _ []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")
	tagName, _ := cmd.Flags().GetString("tag")
	yes, _ := cmd.Flags().GetBool("yes")

	if tagName == "" && !yes {
		confirmed, err := r.survey.AskConfirmUninstall(r.project.CommandName)
		if err != nil {
			return err
		}
		if !confirmed {
			r.logger.Info("Aborted uninstallation ...")
			return nil
		}
	}

	manager, err := getExtensionManager(verbose, r.reservedCommandNames...)
	if err != nil {
		return err
	}

	if tagName == "" {
		r.logger.Info(fmt.Sprintf("Uninstalling [%s] ...", r.project.CommandName))
	} else {
		r.logger.Info(fmt.Sprintf("Uninstalling [%s@%s] ...", r.project.CommandName, tagName))
	}
	if err := manager.Uninstall(r.project.CommandName, tagName); err != nil {
		r.logger.Error("... finished with error")
		return err
	}
	r.logger.Info("... finished successfully")
	return nil
}
