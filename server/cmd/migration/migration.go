package migration

import "github.com/spf13/cobra"

// NewMigrationCommand initializes command for migration
func NewMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migration",
		Short: "Command to do migration activity",
	}
	cmd.AddCommand(NewRollbackCommand())
	cmd.AddCommand(NewMigrateToCommand())
	return cmd
}
