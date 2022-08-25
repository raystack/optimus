package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/internal/connectivity"
	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/cmd/internal/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type listCommand struct {
	logger         log.Logger
	configFilePath string

	projectName string
	host        string
}

// NewListCommand initializes list command for replay
func NewListCommand() *cobra.Command {
	list := &listCommand{}

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

	list.injectFlags(cmd)

	return cmd
}

func (l *listCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&l.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&l.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&l.host, "host", "", "Optimus service endpoint url")
}

func (l *listCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(l.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		l.logger = logger.NewDefaultLogger()
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	l.logger = logger.NewClientLogger(conf.Log)
	if l.projectName == "" {
		l.projectName = conf.Project.Name
	}
	if l.host == "" {
		l.host = conf.Host
	}

	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	conn, err := connectivity.NewConnectivity(l.host, replayTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	replay := pb.NewReplayServiceClient(conn.GetConnection())
	replayStatusRequest := &pb.ListReplaysRequest{
		ProjectName: l.projectName,
	}
	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	replayResponse, err := replay.ListReplays(conn.GetContext(), replayStatusRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.logger.Error("Replay request took too long, timing out")
		}
		return fmt.Errorf("failed to get replay requests: %w", err)
	}
	if len(replayResponse.ReplayList) == 0 {
		l.logger.Warn(fmt.Sprintf("No replays were found in %s project.", l.projectName))
	} else {
		l.printReplayListResponse(replayResponse)
	}
	return nil
}

func (l *listCommand) printReplayListResponse(replayListResponse *pb.ListReplaysResponse) {
	l.logger.Info("Recent replays")
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
		row := []string{
			replaySpec.Id,
			replaySpec.JobName,
			replaySpec.StartDate.AsTime().Format(models.JobDatetimeLayout),
			replaySpec.EndDate.AsTime().Format(models.JobDatetimeLayout),
			replaySpec.Config[models.ConfigIgnoreDownstream],
			replaySpec.CreatedAt.AsTime().Format(time.RFC3339),
			replaySpec.State,
		}
		table.Append(row)
	}

	table.Render()
	l.logger.Info("")
}
