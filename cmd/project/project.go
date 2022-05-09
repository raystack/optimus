package project

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
)

// NewProjectCommand initializes command for project
func NewProjectCommand() *cobra.Command {
	logger := logger.NewDefaultLogger()

	cmd := &cobra.Command{
		Use:     "project",
		Short:   "Commands that will let the user to operate on project",
		Example: "optimus project [sub-command]",
	}
	cmd.AddCommand(NewRegisterCommand(logger))
	cmd.AddCommand(NewDescribeCommand(logger))
	return cmd
}
