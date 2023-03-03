package migration

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/store/postgres"
)

type migrateTo struct {
	configFilePath string
	version        int
}

// NewMigrateToCommand initializes command for migration to a specific version
func NewMigrateToCommand() *cobra.Command {
	to := &migrateTo{}
	cmd := &cobra.Command{
		Use:   "to",
		Short: "Command to migrate to specific migration version",
		RunE:  to.RunE,
	}
	cmd.Flags().StringVarP(&to.configFilePath, "config", "c", to.configFilePath, "File path for server configuration")
	cmd.Flags().IntVarP(&to.version, "version", "v", -1, "Number of migrations to rollback")
	return cmd
}

func (m *migrateTo) RunE(_ *cobra.Command, _ []string) error {
	clientConfig, err := config.LoadServerConfig(m.configFilePath)
	if err != nil {
		return fmt.Errorf("error loading client config: %w", err)
	}

	if m.version < 0 {
		return fmt.Errorf("invalid migration version")
	}

	dsn := clientConfig.Serve.DB.DSN

	fmt.Printf("Executing migration to version %d \n", m.version) // nolint:forbidigo
	err = postgres.ToVersion(uint(m.version), dsn)
	if err != nil {
		return fmt.Errorf("error during migration: %w", err)
	}
	fmt.Println("Migration finished successfully") // nolint:forbidigo
	return nil
}
