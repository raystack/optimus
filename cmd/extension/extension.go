package extension

import (
	"fmt"
	"net/http"

	"github.com/google/go-github/github"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/extension"
)

// UpdateWithExtension updates input command with the available extensions
func UpdateWithExtension(cmd *cobra.Command) {
	httpClient := &http.Client{}
	githubClient := github.NewClient(nil)

	extensionDir, err := extension.GetDefaultDir()
	if err != nil {
		panic(err)
	}
	manifest, err := extension.LoadManifest(extensionDir)
	if err != nil {
		panic(err)
	}

	reservedCommands := getUsedCommands(cmd)

	extension, err := extension.NewExtension(
		manifest,
		githubClient.Repositories, httpClient,
		extensionDir,
		reservedCommands...,
	)
	if err != nil {
		panic(err)
	}

	cmd.AddCommand(newExtensionCommand(extension))
	commands := generateCommands(manifest, extension.Run)
	for _, c := range commands {
		cmd.AddCommand(c)
	}
}

func newExtensionCommand(extension *extension.Extension) *cobra.Command {
	c := &cobra.Command{
		Use:     "extension SUBCOMMAND",
		Aliases: []string{"ext"},
		Short:   "Operate with extension",
	}
	c.AddCommand(newInstallCommand(extension))
	return c
}

func generateCommands(manifest *extension.Manifest, execFn func(string, []string) error) []*cobra.Command {
	output := make([]*cobra.Command, len(manifest.Metadatas))
	for i, metadata := range manifest.Metadatas {
		firstAlias := metadata.Aliases[0]
		c := &cobra.Command{
			Use:     metadata.Aliases[0],
			Aliases: metadata.Aliases,
			Short: fmt.Sprintf("Execute %s/%s [%s] extension",
				metadata.Owner, metadata.Repo, metadata.Tag,
			),
			RunE: func(cmd *cobra.Command, args []string) error {
				return execFn(firstAlias, args)
			},
		}
		c.DisableFlagParsing = true
		output[i] = c
	}
	return output
}

func getUsedCommands(cmd *cobra.Command) []string {
	output := make([]string, len(cmd.Commands()))
	for i, c := range cmd.Commands() {
		output[i] = c.Name()
	}
	return output
}
