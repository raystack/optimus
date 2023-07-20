package replay

import (
	"errors"
	"fmt"
	"time"

	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/raystack/optimus/client/cmd/internal"
	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/client/cmd/internal/progressbar"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const (
	replayTimeout        = time.Minute * 1
	ISOTimeLayout        = time.RFC3339
	pollIntervalInSecond = 30
)

var (
	supportedISOTimeLayouts = [...]string{time.RFC3339, "2006-01-02"}
	terminalStatuses        = map[string]bool{"success": true, "failed": true, "invalid": true}
)

type createCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	parallel    bool
	description string
	jobConfig   string

	projectName   string
	namespaceName string
	host          string
}

// CreateCommand initializes command for creating a replay request
func CreateCommand() *cobra.Command {
	refresh := &createCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Run replay operation on a dag based on provided start and end time range",
		Long: "This operation takes three arguments, first is DAG name[required]\nused in optimus specification, " +
			"second is start time[required] of\nreplay, third is end time[optional] of replay. \nDate ranges are inclusive. " +
			"Supported date formats are RFC3339 and \nsimple date YYYY-MM-DD",
		Example: "optimus replay create <job_name> <2023-01-01T02:30:00Z00:00> [2023-01-02T02:30:00Z00:00]\noptimus replay create <job_name> <2023-01-01> [2023-01-02]",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("job name is required")
			}
			if len(args) < 2 { //nolint: gomnd
				return errors.New("replay start time is required")
			}
			return nil
		},
		RunE:    refresh.RunE,
		PreRunE: refresh.PreRunE,
	}

	refresh.injectFlags(cmd)
	return cmd
}

func (r *createCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&r.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&r.namespaceName, "namespace-name", "n", "", "Name of the optimus namespace")

	cmd.Flags().BoolVarP(&r.parallel, "parallel", "", false, "Backfill job runs in parallel")
	cmd.Flags().StringVarP(&r.description, "description", "d", "", "Description of why backfill is needed")
	cmd.Flags().StringVarP(&r.jobConfig, "job-config", "", "", "additional job configurations")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&r.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&r.host, "host", "", "Optimus service endpoint url")
}

func (r *createCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	conf, err := internal.LoadOptionalConfig(r.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if r.projectName == "" {
		r.projectName = conf.Project.Name
	}
	if r.host == "" {
		r.host = conf.Host
	}

	r.connection = connection.New(r.logger, conf)
	return nil
}

func (r *createCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	startTime := args[1]
	endTime := args[1]
	if len(args) >= 3 { //nolint: gomnd
		endTime = args[2]
	}

	replayID, err := r.createReplayRequest(jobName, startTime, endTime, r.jobConfig)
	if err != nil {
		return err
	}
	r.logger.Info("Replay request is accepted and it is in progress")
	r.logger.Info("Either you could wait or you could close (ctrl+c) and check the status with `optimus replay status %s` command later", replayID)

	return r.waitForReplayState(replayID)
}

func (r *createCommand) waitForReplayState(replayID string) error {
	spinner := progressbar.NewProgressBarWithWriter(r.logger.Writer())
	status := "in progress"
	spinner.Start(fmt.Sprintf("%s...", status))
	for {
		resp, err := r.getReplay(replayID)
		if err != nil {
			return err
		}
		if status != resp.Status {
			status = resp.Status
			spinner.StartNewLine(fmt.Sprintf("%s...", status))
		}
		if _, ok := terminalStatuses[status]; ok {
			spinner.StartNewLine("\n")
			spinner.Stop()
			r.logger.Info("\n" + stringifyReplayStatus(resp))
			break
		}
		time.Sleep(time.Duration(pollIntervalInSecond) * time.Second)
	}
	spinner.Stop()
	return nil
}

func (r *createCommand) getReplay(replayID string) (*pb.GetReplayResponse, error) {
	return getReplay(r.host, replayID, r.connection)
}

func (r *createCommand) createReplayRequest(jobName, startTimeStr, endTimeStr, jobConfig string) (string, error) {
	conn, err := r.connection.Create(r.host)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	replayService := pb.NewReplayServiceClient(conn)

	startTime, err := getTimeProto(startTimeStr)
	if err != nil {
		return "", err
	}
	endTime, err := getTimeProto(endTimeStr)
	if err != nil {
		return "", err
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), replayTimeout)
	defer cancelFunc()

	respStream, err := replayService.Replay(ctx, &pb.ReplayRequest{
		ProjectName:   r.projectName,
		JobName:       jobName,
		NamespaceName: r.namespaceName,
		StartTime:     startTime,
		EndTime:       endTime,
		Parallel:      r.parallel,
		Description:   r.description,
		JobConfig:     jobConfig,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Error("Replay creation took too long, timing out")
		}
		return "", fmt.Errorf("replay request failed: %w", err)
	}
	return respStream.Id, nil
}

func getTimeProto(timeStr string) (*timestamppb.Timestamp, error) {
	var parsedTime time.Time
	var err error
	for _, ISOTimeLayout := range supportedISOTimeLayouts {
		parsedTime, err = time.Parse(ISOTimeLayout, timeStr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return timestamppb.New(parsedTime), nil
}
