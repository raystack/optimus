package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func backupListCommand(l log.Logger, conf config.ProjectConfig, datastoreRepo models.DatastoreRepo) *cli.Command {
	var (
		backupCmd = &cli.Command{
			Use:     "list",
			Short:   "Get list of backups per project and datastore",
			Example: "optimus backup list",
		}
		project string
	)
	backupCmd.Flags().StringVarP(&project, "project", "p", conf.Project.Name, "project name of optimus managed repository")
	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		availableStorer := []string{}
		for _, s := range datastoreRepo.GetAll() {
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

		dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
		defer dialCancel()

		conn, err := createConnection(dialTimeoutCtx, conf.Host)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(ErrServerNotReachable(conf.Host).Error())
			}
			return err
		}
		defer conn.Close()

		requestTimeout, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
		defer requestCancel()

		runtime := pb.NewRuntimeServiceClient(conn)

		spinner := NewProgressBar()
		spinner.Start("please wait...")
		listBackupsResponse, err := runtime.ListBackups(requestTimeout, listBackupsRequest)
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
