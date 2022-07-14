package internal

import "github.com/spf13/cobra"

func MarkFlagsRequired(cmd *cobra.Command, flagNames []string) {
	for _, n := range flagNames {
		cmd.MarkFlagRequired(n)
	}
}
