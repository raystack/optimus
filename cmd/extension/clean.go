package extension

import (
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/survey"
	"github.com/odpf/optimus/extension"
)

type cleanCommand struct {
	logger log.Logger
	survey *survey.ExtensionSurvey
}

func newCleanCommand(logger log.Logger) *cobra.Command {
	clean := &cleanCommand{
		logger: logger,
		survey: survey.NewExtensionSurvey(),
	}
	cmd := &cobra.Command{
		Use:   "clean",
		Short: "clean all extension and its manifest from local",
		Long:  "this command can be used in case manifest is corrupted",
		RunE:  clean.RunE,
	}
	return cmd
}

func (c *cleanCommand) RunE(cmd *cobra.Command, _ []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")
	confirmed, err := c.survey.AskConfirmClean()
	if err != nil {
		return err
	}
	if !confirmed {
		c.logger.Warn("Aborted clean process ...")
		return nil
	}
	return extension.Clean(verbose)
}
