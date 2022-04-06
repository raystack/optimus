package cmd

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

const (
	backupTimeout = time.Minute * 15
)

func backupCommand() *cli.Command {
	var (
		configFilePath string
		conf           config.ClientConfig
	)

	cmd := &cli.Command{
		Use:   "backup",
		Short: "Backup a resource and its downstream",
		Long: heredoc.Doc(`Backup supported resource of a datastore and all of its downstream dependencies.
			Operation can take up to few minutes to complete. It is advised to check the operation status
			using "list" command.
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		c, err := config.LoadClientConfig(configFilePath, cmd.Flags())
		if err != nil {
			return err
		}
		conf = *c

		return nil
	}

	cmd.AddCommand(backupCreateCommand(&conf))
	cmd.AddCommand(backupListCommand(&conf))
	cmd.AddCommand(backupStatusCommand(&conf))
	return cmd
}
