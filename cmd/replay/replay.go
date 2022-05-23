package replay

import (
	"time"

	"github.com/spf13/cobra"

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

	cmd.AddCommand(NewCreateCommand(replay.clientConfig))
	cmd.AddCommand(NewListCommand(replay.clientConfig))
	cmd.AddCommand(NewStatusCommand(replay.clientConfig))
	return cmd
}

func (r *replayCommand) PersistentPreRunE(cmd *cobra.Command, _ []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(r.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*r.clientConfig = *c
	return nil
}