package admin

import (
	"github.com/spf13/cobra"
)

// NewBuildCommand initializes command to build for admin
func NewBuildCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(NewBuildInstanceCommand())
	return cmd
}
