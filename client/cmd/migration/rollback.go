package migration

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/store/postgres"
)

type rollbackCommand struct {
	configFilePath string
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
	return cmd
}

func (r *rollbackCommand) RunE(_ *cobra.Command, _ []string) error {
	clientConfig, err := config.LoadServerConfig(r.configFilePath)
	if err != nil {
		return fmt.Errorf("error loading client config: %w", err)
	}

	l := logger.NewClientLogger()
	dsn := clientConfig.Serve.DB.DSN

	l.Info("initiating migration")
	migration, err := postgres.NewMigration(l, config.BuildVersion, dsn)
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	l.Info("executing rollback")
	if err := migration.Rollback(context.Background()); err != nil {
		return fmt.Errorf("error rolling back migration: %w", err)
	}
	l.Info("rollback finished successfully")
	return nil
}
