package job

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/plugin"
	"github.com/odpf/optimus/config"
)

type jobCommand struct {
	pluginCleanFn func()
}

// NewJobCommand initializes command for job
func NewJobCommand() *cobra.Command {
	job := jobCommand{}

	cmd := &cobra.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
		PersistentPreRunE:  job.PersistentPreRunE,
		PersistentPostRunE: job.PersistentPostRunE,
	}

	cmd.AddCommand(
		NewCreateCommand(),
		NewAddHookCommand(),
		NewRefreshCommand(),
		NewRenderCommand(),
		NewRunListCommand(),
		NewValidateCommand(),
		NewJobRunInputCommand(),
	)
	return cmd
}

func (j *jobCommand) PersistentPreRunE(_ *cobra.Command, _ []string) error {
	// TODO: refactor initialize client deps
	var err error
	j.pluginCleanFn, err = plugin.TriggerClientPluginsInit(config.LogLevelInfo)
	return err
}

func (j *jobCommand) PersistentPostRunE(_ *cobra.Command, _ []string) error {
	j.pluginCleanFn()
	return nil
}
