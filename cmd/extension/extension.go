package extension

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/extension/model"
)

// UpdateWithExtension updates input command with the available extensions
func UpdateWithExtension(cmd *cobra.Command) {
	logger := logger.NewDefaultLogger()
	reservedCommandNames := getReservedCommandNames(cmd)

	cmd.AddCommand(extensionCommand(logger, reservedCommandNames))

	extensionExecCommands := generateRunCommands(reservedCommandNames)
	for _, c := range extensionExecCommands {
		cmd.AddCommand(c)
	}
}

func getReservedCommandNames(cmd *cobra.Command) []string {
	custom := []string{"optimus", "extension"}
	var output []string
	for _, c := range cmd.Commands() {
		output = append(output, c.Name())
	}
	return append(output, custom...)
}

func extensionCommand(logger log.Logger, reservedCommandNames []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "extension SUBCOMMAND",
		Short: "operate on extension",
	}
	cmd.PersistentFlags().BoolP("verbose", "v", false, "if true, then more message will be provided if error encountered")

	cmd.AddCommand(newInstallCommand(logger, reservedCommandNames))
	managementCommands := generateManagementCommands(logger, reservedCommandNames)
	for _, c := range managementCommands {
		cmd.AddCommand(c)
	}

	return cmd
}

func generateManagementCommands(logger log.Logger, reservedCommandNames []string) []*cobra.Command {
	manifester := extension.NewDefaultManifester()
	manifest, err := manifester.Load(model.ExtensionDir)
	if err != nil {
		panic(err)
	}

	var output []*cobra.Command
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			cmd := &cobra.Command{
				Use: project.CommandName,
				Short: fmt.Sprintf("Sub-command to operate over extension [%s/%s@%s]",
					owner.Name, project.Name, project.ActiveTagName,
				),
			}
			cmd.AddCommand(newActivateCommand(logger, project, reservedCommandNames))
			cmd.AddCommand(newDescribeCommand(logger, project))
			cmd.AddCommand(newRenameCommand(logger, project, reservedCommandNames))
			cmd.AddCommand(newUninstallCommand(logger, project, reservedCommandNames))
			cmd.AddCommand(newUpgradeCommand(logger, project, reservedCommandNames))

			output = append(output, cmd)
		}
	}
	return output
}

func getExtensionManager(verbose bool, reservedCommandNames ...string) (*extension.Manager, error) {
	ctx := context.Background()
	httpClient := http.DefaultClient
	manifester := extension.NewDefaultManifester()
	assetOperator := extension.NewDefaultAssetOperator(os.Stdin, os.Stdout, os.Stderr)
	return extension.NewManager(ctx, httpClient, manifester, assetOperator, verbose, reservedCommandNames...)
}
