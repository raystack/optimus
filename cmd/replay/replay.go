package replay

import (
	"time"

	"github.com/spf13/cobra"
)

const (
	replayTimeout      = time.Minute * 15
	defaultProjectName = "sample_project"
)

// NewReplayCommand initializes replay command
func NewReplayCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Re-running jobs in order to update data for older dates/partitions",
		Long:  `Backfill etl job and all of its downstream dependencies`,
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(
		NewCreateCommand(),
		NewListCommand(),
		NewStatusCommand(),
	)
	return cmd
}
