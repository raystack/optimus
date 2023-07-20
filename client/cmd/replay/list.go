package replay

import (
	"bytes"
	"context"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal"
	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

type listCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	projectName string
	host        string
}

func ListCommand() *cobra.Command {
	l := &listCommand{
		logger: logger.NewClientLogger(),
	}
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List down all of the replay based on the given project",
		Example: "optimus list",
		PreRunE: l.PreRunE,
		RunE:    l.RunE,
	}
	l.injectFlags(cmd)
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
	conf, err := internal.LoadOptionalConfig(l.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if l.projectName == "" {
		l.projectName = conf.Project.Name
	}
	if l.host == "" {
		l.host = conf.Host
	}
	l.connection = connection.New(l.logger, conf)
	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	listReplayRequest := &pb.ListReplayRequest{
		ProjectName: l.projectName,
	}
	return l.listReplay(listReplayRequest)
}

func (l *listCommand) listReplay(req *pb.ListReplayRequest) error {
	conn, err := l.connection.Create(l.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	replayService := pb.NewReplayServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), replayTimeout)
	defer cancelFunc()

	listReplayResp, err := replayService.ListReplay(ctx, req)
	if err != nil {
		return err
	}

	if len(listReplayResp.GetReplays()) == 0 {
		l.logger.Info("No replays were found in %s project.", req.ProjectName)
	} else {
		result := stringifyListOfReplays(listReplayResp)
		l.logger.Info("Replays for project: %s", l.projectName)
		l.logger.Info(result)
	}
	return nil
}

func stringifyListOfReplays(resp *pb.ListReplayResponse) string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Job Name",
		"Start Date",
		"End Date",
		"Description",
		"Status",
	})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, replay := range resp.Replays {
		table.Append([]string{
			replay.GetId(),
			replay.GetJobName(),
			replay.GetReplayConfig().GetStartTime().AsTime().Format(time.RFC3339),
			replay.GetReplayConfig().GetEndTime().AsTime().Format(time.RFC3339),
			replay.GetReplayConfig().GetDescription(),
			replay.GetStatus(),
		})
	}
	table.Render()
	return buff.String()
}
