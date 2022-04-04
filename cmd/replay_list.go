package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	cli "github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func replayListCommand(conf *config.ClientConfig) *cli.Command {
	var (
		reCmd = &cli.Command{
			Use:     "list",
			Short:   "Get list of a replay mapping to a project",
			Example: "optimus replay list",
			Long: `
The list command is used to fetch the recent replay in one project. 
		`,
		}
	)
	reCmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	reCmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initClientLogger(conf.Log)
		ctx, conn, closeConn, err := initClientConnection(l, conf.Host, replayTimeout)
		if err != nil {
			return err
		}
		defer closeConn()

		replay := pb.NewReplayServiceClient(conn)
		replayStatusRequest := &pb.ListReplaysRequest{
			ProjectName: conf.Project.Name,
		}
		spinner := NewProgressBar()
		spinner.Start("please wait...")
		replayResponse, err := replay.ListReplays(ctx, replayStatusRequest)
		spinner.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Replay request took too long, timing out"))
			}
			return fmt.Errorf("failed to get replay requests: %w", err)
		}
		if len(replayResponse.ReplayList) == 0 {
			l.Info(fmt.Sprintf("No replays were found in %s project.", conf.Project.Name))
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
		table.Append([]string{
			replaySpec.Id, replaySpec.JobName, replaySpec.StartDate.AsTime().Format(models.JobDatetimeLayout),
			replaySpec.EndDate.AsTime().Format(models.JobDatetimeLayout), replaySpec.Config[models.ConfigIgnoreDownstream],
			replaySpec.CreatedAt.AsTime().Format(time.RFC3339), replaySpec.State,
		})
	}

	table.Render()
	l.Info("")
}
