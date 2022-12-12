package scheduler

import (
	"github.com/spf13/cobra"
)

// NewSchedulerCommand initializes command for job
func NewSchedulerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scheduler",
		Short: "scheduled/run job related functions",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(
		UploadCommand(),
		NewJobRunInputCommand(),
	)
	return cmd
}
