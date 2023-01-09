package resource

import (
	"github.com/spf13/cobra"
)

// NewResourceCommand initializes command for resource
func NewResourceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource",
		Short: "Interact with data resource",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewUploadAllCommand())
	return cmd
}
