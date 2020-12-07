package commands

import (
	cli "github.com/spf13/cobra"
)

func versionCommand(l logger, v string) *cli.Command {
	// Version prints the current version of the tool.
	return &cli.Command{
		Use:   "version",
		Short: "Print the client version information",
		Run: func(c *cli.Command, args []string) {
			l.Println(v)
		},
	}
}
