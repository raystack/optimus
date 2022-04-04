package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	cli "github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func backupStatusCommand(conf *config.ClientConfig) *cli.Command {
	var (
		backupCmd = &cli.Command{
			Use:     "status",
			Short:   "Get backup info using uuid and datastore",
			Example: "optimus backup status <uuid>",
		}
	)
	backupCmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
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

		listBackupsRequest := &pb.GetBackupRequest{
			ProjectName:   project,
			DatastoreName: storerName,
			Id:            args[0],
		}

		ctx, conn, closeConn, err := initClientConnection(l, conf.Host, backupTimeout)
		if err != nil {
			return err
		}
		defer closeConn()

		backup := pb.NewBackupServiceClient(conn)

		spinner := NewProgressBar()
		spinner.Start("please wait...")
		backupDetailResponse, err := backup.GetBackup(ctx, listBackupsRequest)
		spinner.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Getting backup detail took too long, timing out"))
			}
			return fmt.Errorf("request failed to get backup detail: %w", err)
		}

		printBackupDetailResponse(l, backupDetailResponse)
		return nil
	}
	return backupCmd
}

func printBackupDetailResponse(l log.Logger, backupDetailResponse *pb.GetBackupResponse) {
	l.Info("")
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)

	ttl := backupDetailResponse.Spec.Config[models.ConfigTTL]
	expiry := backupDetailResponse.Spec.CreatedAt.AsTime()
	if ttl != "" {
		ttlDuration, err := time.ParseDuration(ttl)
		if err != nil {
			l.Error(coloredError("Unable to parse backup TTL: %v", err))
		} else {
			expiry = expiry.Add(ttlDuration)
		}
	}

	table.Append([]string{"ID", backupDetailResponse.Spec.Id})
	table.Append([]string{"Resource", backupDetailResponse.Spec.ResourceName})
	table.Append([]string{"Created at", backupDetailResponse.Spec.CreatedAt.AsTime().Format(time.RFC3339)})
	table.Append([]string{"Ignore downstream?", backupDetailResponse.Spec.Config[models.ConfigIgnoreDownstream]})
	table.Append([]string{"Expire at", expiry.Format(time.RFC3339)})
	table.Append([]string{"Description", backupDetailResponse.Spec.Description})
	table.Append([]string{"Result", strings.Join(backupDetailResponse.Urn, "\n")})
	table.Render()
}
