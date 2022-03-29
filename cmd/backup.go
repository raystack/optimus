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

func backupCommand(l log.Logger, conf config.Optimus, datastoreRepo models.DatastoreRepo) *cli.Command {
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
	cmd.AddCommand(backupCreateCommand(l, conf, datastoreRepo))
	cmd.AddCommand(backupListCommand(l, conf, datastoreRepo))
	cmd.AddCommand(backupStatusCommand(l, conf, datastoreRepo))
	return cmd
}
