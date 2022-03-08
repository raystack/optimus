package cmd

import (
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

const (
	backupTimeout = time.Minute * 15
)

func backupCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Optimus) *cli.Command {
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
	cmd.AddCommand(backupCreateCommand(l, datastoreRepo, conf))
	cmd.AddCommand(backupListCommand(l, datastoreRepo, conf))
	cmd.AddCommand(backupStatusCommand(l, datastoreRepo, conf))
	return cmd
}
