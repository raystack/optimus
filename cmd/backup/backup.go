package backup

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	backupTimeout      = time.Minute * 15
	defaultProjectName = "sample_project"
)

type backCommand struct {
	clientConfig *config.ClientConfig

	configFilePath string
}

// NewBackupCommand initializes
func NewBackupCommand() *cobra.Command {
	backup := &backCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup a resource and its downstream",
		Long: heredoc.Doc(`Backup supported resource of a datastore and all of its downstream dependencies.
			Operation can take up to few minutes to complete. It is advised to check the operation status
			using "list" command.
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		PersistentPreRunE: backup.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&backup.configFilePath, "config", "c", backup.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewCreateCommand(backup.clientConfig))
	cmd.AddCommand(NewListCommand(backup.clientConfig))
	cmd.AddCommand(NewStatusCommand(backup.clientConfig))
	return cmd
}

func (b *backCommand) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(b.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*b.clientConfig = *c
	return nil
}

func getAvailableDatastorers() []string {
	dsRepo := models.DatastoreRegistry
	availableStorers := []string{}
	for _, s := range dsRepo.GetAll() {
		availableStorers = append(availableStorers, s.Name())
	}
	return availableStorers
}
