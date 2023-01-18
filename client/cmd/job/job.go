package job

import (
	"github.com/spf13/cobra"
)

// NewJobCommand initializes command for job
func NewJobCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(
		NewCreateCommand(),
		NewAddHookCommand(),
		NewRefreshCommand(),
		NewRunListCommand(),
		NewValidateCommand(),
		NewInspectCommand(),
		NewReplaceAllCommand(),
		NewExportCommand(),
		NewJobRunInputCommand(),
	)
	return cmd
}
