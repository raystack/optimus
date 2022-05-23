package admin

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

// NewBuildCommand initializes command to build for admin
func NewBuildCommand(clientConfig *config.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(NewBuildInstanceCommand(clientConfig))
	return cmd
}
