package replay

import (
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	replayTimeout = time.Minute * 60
	ISOTimeLayout = time.RFC3339
)

type createCommand struct {
	logger         log.Logger
	configFilePath string

	parallel bool

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
			"second is start time[required] of\nreplay, third is end time[optional] of replay. \nDate ranges are inclusive.",
		Example: "optimus replay create <job_name> <2023-01-01T02:30:00Z00:00> [2023-01-02T02:30:00Z00:00]",
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

	cmd.Flags().BoolVarP(&r.parallel, "parallel", "", false, "backfill job runs in parallel")

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
	return nil
}

func (r *createCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	startTime := args[1]
	endTime := args[1]
	if len(args) >= 3 { //nolint: gomnd
		endTime = args[2]
	}

	replayID, err := r.createReplayRequest(jobName, startTime, endTime)
	if err != nil {
		return err
	}
	r.logger.Info("Replay request created with id %s", replayID)
	return nil
}

func (r *createCommand) createReplayRequest(jobName, startTimeStr, endTimeStr string) (string, error) {
	conn, err := connectivity.NewConnectivity(r.host, replayTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	replayService := pb.NewReplayServiceClient(conn.GetConnection())

	startTime, err := getTimeProto(startTimeStr)
	if err != nil {
		return "", err
	}
	endTime, err := getTimeProto(endTimeStr)
	if err != nil {
		return "", err
	}
	respStream, err := replayService.Replay(conn.GetContext(), &pb.ReplayRequest{
		ProjectName:   r.projectName,
		JobName:       jobName,
		NamespaceName: r.namespaceName,
		StartTime:     startTime,
		EndTime:       endTime,
		Parallel:      r.parallel,
		Description:   "",
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
	parsedTime, err := time.Parse(ISOTimeLayout, timeStr)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(parsedTime), nil
}
