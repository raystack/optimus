package extension

import (
	"fmt"

	"github.com/spf13/cobra"
)

func generateRunCommands(reservedCommandNames []string) []*cobra.Command {
	manifest := loadManifest()

	var output []*cobra.Command
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			c := &cobra.Command{
				Use: project.CommandName,
				Short: fmt.Sprintf("Execute [%s/%s@%s] extension",
					owner.Name, project.Name, project.ActiveTagName,
				),
				RunE: func(cmd *cobra.Command, args []string) error {
					manager, err := getExtensionManager(true, reservedCommandNames...)
					if err != nil {
						return err
					}
					return manager.Run(project.CommandName, args...)
				},
			}
			c.DisableFlagParsing = true
			output = append(output, c)
		}
	}
	return output
}
