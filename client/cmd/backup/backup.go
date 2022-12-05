package backup

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/spf13/cobra"
)

const (
	backupTimeout = time.Minute * 15
)

// NewBackupCommand initializes
func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup a list resource",
		Long: heredoc.Doc(`Backup supported resources of a datastore.
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
