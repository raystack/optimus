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
	var configFilePath string
	conf := &config.ClientConfig{}
	l := initLogger(plainLoggerType, conf.Log)

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
	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		var err error

		conf, err = config.LoadClientConfig(configFilePath)
		if err != nil {
			return err
		}
		l = initLogger(plainLoggerType, conf.Log)

		return nil
	}

	cmd.AddCommand(backupCreateCommand(l, conf))
	cmd.AddCommand(backupListCommand(l, conf))
	cmd.AddCommand(backupStatusCommand(l, conf))
	return cmd
}
