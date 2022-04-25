package job

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/config"
)

type job struct {
	configFilePath string
	clientConfig   *config.ClientConfig

	pluginCleanFn func()
}

// NewJobCommand initializes command for job
func NewJobCommand() *cobra.Command {
	j := job{
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
	cmd.PersistentPreRunE = j.PersistentPreRunE
	cmd.PersistentPostRunE = j.PersistentPostRunE
	cmd.PersistentFlags().StringVarP(&j.configFilePath, "config", "c", j.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewCreateCommand(logger, j.clientConfig))
	// cmd.AddCommand(jobAddHookCommand(&conf))
	// cmd.AddCommand(jobRenderTemplateCommand(&conf))
	// cmd.AddCommand(jobValidateCommand(&conf))
	// cmd.AddCommand(jobRunCommand(&conf))
	// cmd.AddCommand(jobRunListCommand(&conf))
	// cmd.AddCommand(jobRefreshCommand(&conf))

	return nil
}

func (j *job) PersistentPreRunE(cmd *cobra.Command, args []string) error {
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

func (j *job) PersistentPostRunE(cmd *cobra.Command, args []string) error {
	j.pluginCleanFn()
	return nil
}
