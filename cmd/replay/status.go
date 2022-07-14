package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"github.com/xlab/treeprint"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type statusCommand struct {
	logger         log.Logger
	configFilePath string

	projectName string
	host        string
}

// NewStatusCommand initializes command for replay status
func NewStatusCommand() *cobra.Command {
	status := &statusCommand{}

	cmd := &cobra.Command{
		Use:     "status",
		Short:   "Get status of a replay using its ID",
		Example: "optimus replay status <replay-uuid>",
		Long: `
The status command is used to fetch the current replay progress.
It takes one argument, replay ID[required] that gets generated when starting a replay. 
		`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("replay ID is required")
			}
			return nil
		},
		RunE:    status.RunE,
		PreRunE: status.PreRunE,
	}

	status.injectFlags(cmd)

	return cmd
}

func (s *statusCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&s.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&s.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&s.host, "host", "", "Optimus service endpoint url")
}

func (s *statusCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(s.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		s.logger = logger.NewDefaultLogger()
		markFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	s.logger = logger.NewClientLogger(conf.Log)
	if s.projectName == "" {
		s.projectName = conf.Project.Name
	}
	if s.host == "" {
		s.host = conf.Host
	}

	return nil
}

func (s *statusCommand) RunE(_ *cobra.Command, args []string) error {
	conn, err := connectivity.NewConnectivity(s.host, replayTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	replay := pb.NewReplayServiceClient(conn.GetConnection())
	replayStatusRequest := &pb.GetReplayStatusRequest{
		Id:          args[0],
		ProjectName: s.projectName,
	}
	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	replayResponse, err := replay.GetReplayStatus(conn.GetContext(), replayStatusRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Error(logger.ColoredError("Replay request took too long, timing out"))
		}
		return fmt.Errorf("request getting status for replay %s is failed: %w", args[0], err)
	}
	s.printReplayStatusResponse(replayResponse)
	return nil
}

func (s *statusCommand) printReplayStatusResponse(replayStatusResponse *pb.GetReplayStatusResponse) {
	if replayStatusResponse.State == models.ReplayStatusFailed {
		s.logger.Info(fmt.Sprintf("\nThis replay has been marked as %s", logger.ColoredNotice(models.ReplayStatusFailed)))
	} else if replayStatusResponse.State == models.ReplayStatusReplayed {
		s.logger.Info(fmt.Sprintf("\nThis replay is still %s", logger.ColoredNotice("running")))
	} else if replayStatusResponse.State == models.ReplayStatusSuccess {
		s.logger.Info(fmt.Sprintf("\nThis replay has been marked as %s", logger.ColoredSuccess(models.ReplayStatusSuccess)))
	}
	s.logger.Info(logger.ColoredNotice("Latest Instances Status"))
	s.logger.Info(s.printStatusTree(replayStatusResponse.Response, treeprint.New()).String())
}

func (s *statusCommand) printStatusTree(instance *pb.ReplayStatusTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		runBranch.AddNode(fmt.Sprintf("%s (%s)", run.Run.AsTime().Format(time.RFC3339), run.State))
	}

	for _, childInstance := range instance.Dependents {
		s.printStatusTree(childInstance, subtree)
	}
	return tree
}
