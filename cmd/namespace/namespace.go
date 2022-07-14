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

// GetAllowedDownstreamNamespaces gets all downstream namespace names
func GetAllowedDownstreamNamespaces(namespaceName string, allDownstream bool) []string {
	if allDownstream {
		return []string{"*"}
	}
	return []string{namespaceName}
}

// TODO: move it to another common package, eg. internal
func markFlagsRequired(cmd *cobra.Command, flagNames []string) {
	for _, n := range flagNames {
		cmd.MarkFlagRequired(n)
	}
}
