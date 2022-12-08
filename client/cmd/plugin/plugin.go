package plugin

import (
	"github.com/spf13/cobra"
)

func NewPluginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage plugins",
		Annotations: map[string]string{
			"group:dev": "true",
		},
	}
	cmd.AddCommand(
		NewInstallCommand(),
		NewValidateCommand(),
		NewSyncCommand(),
	)
	return cmd
}
