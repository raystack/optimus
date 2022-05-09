package namespace

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
)

// NewNamespaceCommand initializes command for namespace
func NewNamespaceCommand() *cobra.Command {
	logger := logger.NewDefaultLogger()

	cmd := &cobra.Command{
		Use:     "namespace",
		Short:   "Commands that will let the user to operate on namespace",
		Example: "optimus namespace [sub-command]",
	}
	cmd.AddCommand(NewRegisterCommand(logger))
	cmd.AddCommand(NewDescribeCommand(logger))
	cmd.AddCommand(NewListCommand(logger))
	return cmd
}
