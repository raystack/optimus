package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	cli "github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func backupListCommand(conf *config.ClientConfig) *cli.Command {
	var (
		backupCmd = &cli.Command{
			Use:     "list",
			Short:   "Get list of backups per project and datastore",
			Example: "optimus backup list",
		}
	)
	backupCmd.Flags().StringP("project-name", "p", defaultProjectName, "project name of optimus managed repository")
	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		project := conf.Project.Name
		l := initClientLogger(conf.Log)
		dsRepo := models.DatastoreRegistry
		availableStorer := []string{}
		for _, s := range dsRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastore?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return err
		}

		listBackupsRequest := &pb.ListBackupsRequest{
			ProjectName:   project,
			DatastoreName: storerName,
		}

		ctx, conn, closeConn, err := initClientConnection(conf.Host, backupTimeout)
		if err != nil {
			return err
		}
		defer closeConn()

		backup := pb.NewBackupServiceClient(conn)

		spinner := NewProgressBar()
		spinner.Start("please wait...")
		listBackupsResponse, err := backup.ListBackups(ctx, listBackupsRequest)
		spinner.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Getting list of backups took too long, timing out"))
				return err
			}
			return fmt.Errorf("request failed to get list of backups: %w", err)
		}

		if len(listBackupsResponse.Backups) == 0 {
			l.Info(coloredNotice("No backups were found in %s project.", project))
		} else {
			printBackupListResponse(l, listBackupsResponse)
		}
		return nil
	}
	return backupCmd
}

func printBackupListResponse(l log.Logger, listBackupsResponse *pb.ListBackupsResponse) {
	l.Info(coloredNotice("Recent backups"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Resource",
		"Created at",
		"Ignore Downstream?",
		"TTL",
		"Description",
	})

	for _, backupSpec := range listBackupsResponse.Backups {
		ignoreDownstream := backupSpec.Config[models.ConfigIgnoreDownstream]
		ttl := backupSpec.Config[models.ConfigTTL]
		table.Append([]string{
			backupSpec.Id,
			backupSpec.ResourceName,
			backupSpec.CreatedAt.AsTime().Format(time.RFC3339),
			ignoreDownstream,
			ttl,
			backupSpec.Description,
		})
	}
	table.Render()
	l.Info("")
}
