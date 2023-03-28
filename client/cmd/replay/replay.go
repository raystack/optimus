package replay

import (
	"github.com/spf13/cobra"
)

// NewReplayCommand initializes command for replay
func NewReplayCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "replay related functions",
		Annotations: map[string]string{
			"group:core": "false",
		},
	}

	cmd.AddCommand(
		CreateCommand(),
	)
	return cmd
}
