package backup

import (
	"fmt"
	"strings"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/survey"
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

func prepareDatastoreName(datastoreName string) error {
	availableStorers := getAvailableDatastorers()
	if datastoreName == "" {
		storerName, err := survey.AskToSelectDatastorer(availableStorers)
		if err != nil {
			return err
		}
		datastoreName = storerName
	}
	datastoreName = strings.ToLower(datastoreName)
	validStore := false
	for _, s := range availableStorers {
		if s == datastoreName {
			validStore = true
		}
	}
	if !validStore {
		return fmt.Errorf("invalid datastore type, available values are: %v", availableStorers)
	}
	return nil
}
