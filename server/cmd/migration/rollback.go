package migration

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/store/postgres"
)

type rollbackCommand struct {
	configFilePath string
	count          int
}

// NewRollbackCommand initializes command for migration rollback
func NewRollbackCommand() *cobra.Command {
	rollback := &rollbackCommand{}
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Command to rollback the current active migration",
		RunE:  rollback.RunE,
	}
	cmd.Flags().StringVarP(&rollback.configFilePath, "config", "c", rollback.configFilePath, "File path for server configuration")
	cmd.Flags().IntVarP(&rollback.count, "count", "n", 1, "Number of migrations to rollback")
	return cmd
}

func (r *rollbackCommand) RunE(_ *cobra.Command, _ []string) error {
	clientConfig, err := config.LoadServerConfig(r.configFilePath)
	if err != nil {
		return fmt.Errorf("error loading client config: %w", err)
	}

	dsn := clientConfig.Serve.DB.DSN

	fmt.Printf("Executing rollback for %d migrations\n", r.count) // nolint:forbidigo
	err = postgres.Rollback(dsn, r.count)
	if err != nil {
		return fmt.Errorf("error rolling back migration: %w", err)
	}
	fmt.Println("Rollback finished successfully") // nolint:forbidigo
	return nil
}
