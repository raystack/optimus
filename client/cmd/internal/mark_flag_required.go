package internal

import "github.com/spf13/cobra"

// TODO: need to do refactor for proper file naming
func MarkFlagsRequired(cmd *cobra.Command, flagNames []string) {
	for _, n := range flagNames {
		cmd.MarkFlagRequired(n)
	}
}
