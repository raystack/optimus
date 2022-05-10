package replay

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
)

const (
	replayTimeout      = time.Minute * 15
	defaultProjectName = "sample_project"
)

type replayCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig
}

// NewReplayCommand initializes replay command
func NewReplayCommand() *cobra.Command {
	replay := &replayCommand{
		clientConfig: &config.ClientConfig{},
	}
	logger := logger.NewDefaultLogger()

	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Re-running jobs in order to update data for older dates/partitions",
		Long:  `Backfill etl job and all of its downstream dependencies`,
		Annotations: map[string]string{
			"group:core": "true",
		},
		PersistentPreRunE: replay.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&replay.configFilePath, "config", "c", replay.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewCreateCommand(logger, replay.clientConfig))
	cmd.AddCommand(NewStatusCommand(logger, replay.clientConfig))
	cmd.AddCommand(NewListCommand(logger, replay.clientConfig))
	return cmd
}

func (r *replayCommand) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(r.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*r.clientConfig = *c
	return nil
}
