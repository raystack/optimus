package cmd

import (
	"context"
	"fmt"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
)

func replayListCommand(l log.Logger, conf config.Optimus) *cli.Command {
	var (
		projectName string
		reCmd       = &cli.Command{
			Use:     "list",
			Short:   "Get list of a replay mapping to a project",
			Example: "optimus replay list",
			Long: `
The list command is used to fetch the recent replay in one project. 
		`,
		}
	)
	reCmd.Flags().StringVarP(&projectName, "project", "p", conf.GetProject().Name, "Project name of optimus managed repository")
	reCmd.RunE = func(cmd *cli.Command, args []string) error {
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

		replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
		defer replayRequestCancel()

		runtime := pb.NewRuntimeServiceClient(conn)
		replayStatusRequest := &pb.ListReplaysRequest{
			ProjectName: projectName,
		}
		spinner := NewProgressBar()
		spinner.Start("please wait...")
		replayResponse, err := runtime.ListReplays(replayRequestTimeout, replayStatusRequest)
		spinner.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Replay request took too long, timing out"))
			}
			return errors.Wrapf(err, "failed to get replay requests")
		}
		if len(replayResponse.ReplayList) == 0 {
			l.Info(fmt.Sprintf("No replays were found in %s project.", projectName))
		} else {
			printReplayListResponse(l, replayResponse)
		}
		return nil
	}
	return reCmd
}

func printReplayListResponse(l log.Logger, replayListResponse *pb.ListReplaysResponse) {
	l.Info(coloredNotice("Recent replays"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Job",
		"Start Date",
		"End Date",
		"Ignore Downstream?",
		"Requested At",
		"Status",
	})

	for _, replaySpec := range replayListResponse.ReplayList {
		table.Append([]string{replaySpec.Id, replaySpec.JobName, replaySpec.StartDate.AsTime().Format(models.JobDatetimeLayout),
			replaySpec.EndDate.AsTime().Format(models.JobDatetimeLayout), replaySpec.Config[models.ConfigIgnoreDownstream],
			replaySpec.CreatedAt.AsTime().Format(time.RFC3339), replaySpec.State})
	}

	table.Render()
	l.Info("")
}
