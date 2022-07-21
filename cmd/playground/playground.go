package playground

import (
	"github.com/spf13/cobra"
)

// acts a folder for other playground commands
// NewJobCommand initializes command for job
func NewPlaygroundCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "playground",
		Short: "play around with certain functions",
	}
	cmd.AddCommand(NewPlaygroundWindowCommand())
	return cmd

}
