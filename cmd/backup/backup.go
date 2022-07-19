package backup

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/models"
)

const (
	backupTimeout = time.Minute * 15
)

// NewBackupCommand initializes
func NewBackupCommand() *cobra.Command {
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
	}

	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewStatusCommand())
	return cmd
}

func getAvailableDatastorers() []string {
	dsRepo := models.DatastoreRegistry
	availableStorers := []string{}
	for _, s := range dsRepo.GetAll() {
		availableStorers = append(availableStorers, s.Name())
	}
	return availableStorers
}
