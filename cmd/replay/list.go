package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type listCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig
}

// NewListCommand initializes list command for replay
func NewListCommand(clientConfig *config.ClientConfig) *cobra.Command {
	list := &listCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Get list of a replay mapping to a project",
		Example: "optimus replay list",
		Long: `
The list command is used to fetch the recent replay in one project. 
	`,
		RunE:    list.RunE,
		PreRunE: list.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	return cmd
}

func (l *listCommand) PreRunE(_ *cobra.Command, _ []string) error {
	l.logger = logger.NewClientLogger(l.clientConfig.Log)
	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	conn, err := connectivity.NewConnectivity(l.clientConfig.Host, replayTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	replay := pb.NewReplayServiceClient(conn.GetConnection())
	replayStatusRequest := &pb.ListReplaysRequest{
		ProjectName: l.clientConfig.Project.Name,
	}
	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	replayResponse, err := replay.ListReplays(conn.GetContext(), replayStatusRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.logger.Error(logger.ColoredError("Replay request took too long, timing out"))
		}
		return fmt.Errorf("failed to get replay requests: %w", err)
	}
	if len(replayResponse.ReplayList) == 0 {
		l.logger.Info(fmt.Sprintf("No replays were found in %s project.", l.clientConfig.Project.Name))
	} else {
		l.printReplayListResponse(replayResponse)
	}
	return nil
}

func (l *listCommand) printReplayListResponse(replayListResponse *pb.ListReplaysResponse) {
	l.logger.Info(logger.ColoredNotice("Recent replays"))
	table := tablewriter.NewWriter(l.logger.Writer())
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
	l.logger.Info("")
}
