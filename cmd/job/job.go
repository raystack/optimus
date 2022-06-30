package job

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/config"
)

const (
	defaultProjectName = "sample_project"
	defaultHost        = "localhost:9100"
)

type jobCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig

	pluginCleanFn func()
}

// NewJobCommand initializes command for job
func NewJobCommand() *cobra.Command {
	job := jobCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
		PersistentPreRunE:  job.PersistentPreRunE,
		PersistentPostRunE: job.PersistentPostRunE,
	}
	cmd.PersistentFlags().StringVarP(&job.configFilePath, "config", "c", job.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewCreateCommand(job.clientConfig))
	cmd.AddCommand(NewAddHookCommand(job.clientConfig))
	cmd.AddCommand(NewRefreshCommand(job.clientConfig))
	cmd.AddCommand(NewRenderCommand(job.clientConfig))
	cmd.AddCommand(NewRunListCommand(job.clientConfig))
	cmd.AddCommand(NewValidateCommand(job.clientConfig))
	return cmd
}

func (j *jobCommand) PersistentPreRunE(cmd *cobra.Command, _ []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(j.configFilePath)
	if err != nil {
		return err
	}
	*j.clientConfig = *c

	// TODO: refactor initialize client deps
	j.pluginCleanFn, err = plugin.TriggerClientPluginsInit(j.clientConfig.Log.Level)
	return err
}

func (j *jobCommand) PersistentPostRunE(_ *cobra.Command, _ []string) error {
	j.pluginCleanFn()
	return nil
}
