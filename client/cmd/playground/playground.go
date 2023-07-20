package playground

import (
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/playground/window"
)

// NewPlaygroundCommand initializes command for playground
func NewPlaygroundCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "playground",
		Short: "Play around with some Optimus features",
	}
	cmd.AddCommand(window.NewCommand())
	return cmd
}
