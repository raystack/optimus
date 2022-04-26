package job

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/config"
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
	logger := logger.NewDefaultLogger()

	cmd := &cobra.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}
	cmd.PersistentPreRunE = job.PersistentPreRunE
	cmd.PersistentPostRunE = job.PersistentPostRunE
	cmd.PersistentFlags().StringVarP(&job.configFilePath, "config", "c", job.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewCreateCommand(logger, job.clientConfig))
	cmd.AddCommand(NewAddHookCommand(logger))
	cmd.AddCommand(NewRenderCommand(logger, job.clientConfig))
	// cmd.AddCommand(jobValidateCommand(&conf))
	// cmd.AddCommand(jobRunCommand(&conf))
	// cmd.AddCommand(jobRunListCommand(&conf))
	// cmd.AddCommand(jobRefreshCommand(&conf))

	return nil
}

func (j *jobCommand) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(j.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*j.clientConfig = *c

	// TODO: refactor initialize client deps
	j.pluginCleanFn, err = plugin.TriggerClientPluginsInit(j.clientConfig.Log.Level)
	return err
}

func (j *jobCommand) PersistentPostRunE(cmd *cobra.Command, args []string) error {
	j.pluginCleanFn()
	return nil
}
