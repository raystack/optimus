package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/salt/log"

	"github.com/google/go-github/github"
	cli "github.com/spf13/cobra"
)

func addExtensionCommand(cmd *cli.Command, l log.Logger) {
	ctx := context.Background()
	httpClient := &http.Client{}
	githubClient := github.NewClient(nil)
	manifest, err := extension.LoadManifest()
	if err != nil {
		panic(err)
	}
	reservedCommands := getUsedCommands(cmd)
	extension := extension.NewExtension(ctx, manifest, githubClient, httpClient, reservedCommands...)

	cmd.AddCommand(extensionCommand(extension.Install, l))
	commands := generateCommands(manifest, extension.Run)
	for _, c := range commands {
		cmd.AddCommand(c)
	}
}

func generateCommands(manifest *extension.Manifest, execFn func(string, []string) error) []*cli.Command {
	output := make([]*cli.Command, len(manifest.Metadatas))
	for i, metadata := range manifest.Metadatas {
		c := &cli.Command{
			Use:     metadata.Aliases[0],
			Aliases: metadata.Aliases,
			Short: fmt.Sprintf("Execute %s/%s [%s] extension",
				metadata.Owner, metadata.Repo, metadata.Tag,
			),
			RunE: func(cmd *cli.Command, args []string) error {
				return execFn(metadata.Aliases[0], args)
			},
		}
		c.DisableFlagParsing = true
		output[i] = c
	}
	return output
}

func getUsedCommands(cmd *cli.Command) []string {
	output := make([]string, len(cmd.Commands()))
	for i, c := range cmd.Commands() {
		output[i] = c.Name()
	}
	return output
}

func extensionCommand(installFn func(owner, repo, alias string) error, l log.Logger) *cli.Command {
	c := &cli.Command{
		Use:     "extension SUBCOMMAND",
		Aliases: []string{"ext"},
		Short:   "Operate with extension",
	}
	c.AddCommand(
		func() *cli.Command {
			installCmd := &cli.Command{
				Use:   "install OWNER/REPO",
				Short: "Install an extension",
				RunE: func(cmd *cli.Command, args []string) error {
					if len(args) != 1 {
						return errors.New("one argument for [owner/repo] is required")
					}
					splitArg := strings.Split(args[0], "/")
					if len(splitArg) != 2 || splitArg[0] == "" || splitArg[1] == "" {
						return errors.New("argument should follow pattern [owner/repo]")
					}

					owner := splitArg[0]
					repo := splitArg[1]
					alias, _ := cmd.Flags().GetString("alias")

					l.Info(fmt.Sprintf("Installing %s/%s ...", owner, repo))
					err := installFn(owner, repo, alias)
					if err != nil {
						return err
					}
					l.Info("... success")
					return nil
				},
			}
			installCmd.Flags().StringP("alias", "a", "", "alias to be set for the extension")
			return installCmd
		}(),
	)
	return c
}
