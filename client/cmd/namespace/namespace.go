package namespace

import (
	"github.com/spf13/cobra"
)

// NewNamespaceCommand initializes command for namespace
func NewNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "namespace",
		Short:   "Commands that will let the user to operate on namespace",
		Example: "optimus namespace [sub-command]",
	}
	cmd.AddCommand(
		NewRegisterCommand(),
		NewDescribeCommand(),
		NewListCommand(),
	)
	return cmd
}
