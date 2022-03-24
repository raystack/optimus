package cmd

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	backupTimeout = time.Minute * 15
)

func backupCommand(datastoreRepo models.DatastoreRepo) *cli.Command {
	var configFilePath string
	var conf = &config.ClientConfig{}
	var l log.Logger = initLogger(plainLoggerType, conf.Log)

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

	cmd.AddCommand(backupCreateCommand(l, conf, datastoreRepo))
	cmd.AddCommand(backupListCommand(l, conf, datastoreRepo))
	cmd.AddCommand(backupStatusCommand(l, conf, datastoreRepo))
	return cmd
}
