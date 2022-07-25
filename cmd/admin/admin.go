package admin

import (
	"github.com/spf13/cobra"
)

// NewAdminCommand initializes command for admin
func NewAdminCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "admin",
		Short:  "Internal administration commands",
		Hidden: true,
	}

	cmd.AddCommand(NewBuildCommand())
	return cmd
}
