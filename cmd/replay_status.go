package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"github.com/xlab/treeprint"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func replayStatusCommand(l log.Logger, conf config.Optimus) *cli.Command {
	var projectName string

	reCmd := &cli.Command{
		Use:     "status",
		Short:   "Get status of a replay using its ID",
		Example: "optimus replay status <replay-uuid>",
		Long: `
The status command is used to fetch the current replay progress.
It takes one argument, replay ID[required] that gets generated when starting a replay. 
		`,
		Args: func(cmd *cli.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("replay ID is required")
			}
			return nil
		},
	}
	reCmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "project name of optimus managed repository")
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
		replayStatusRequest := &pb.GetReplayStatusRequest{
			Id:          args[0],
			ProjectName: projectName,
		}
		spinner := NewProgressBar()
		spinner.Start("please wait...")
		replayResponse, err := runtime.GetReplayStatus(replayRequestTimeout, replayStatusRequest)
		spinner.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Replay request took too long, timing out"))
			}
			return fmt.Errorf("request getting status for replay %s is failed: %w", args[0], err)
		}
		printReplayStatusResponse(l, replayResponse)
		return nil
	}
	return reCmd
}

func printReplayStatusResponse(l log.Logger, replayStatusResponse *pb.GetReplayStatusResponse) {
	if replayStatusResponse.State == models.ReplayStatusFailed {
		l.Info(fmt.Sprintf("\nThis replay has been marked as %s", coloredNotice(models.ReplayStatusFailed)))
	} else if replayStatusResponse.State == models.ReplayStatusReplayed {
		l.Info(fmt.Sprintf("\nThis replay is still %s", coloredNotice("running")))
	} else if replayStatusResponse.State == models.ReplayStatusSuccess {
		l.Info(fmt.Sprintf("\nThis replay has been marked as %s", coloredSuccess(models.ReplayStatusSuccess)))
	}
	l.Info(coloredNotice("Latest Instances Status"))
	l.Info(printStatusTree(replayStatusResponse.Response, treeprint.New()).String())
}

func printStatusTree(instance *pb.ReplayStatusTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		runBranch.AddNode(fmt.Sprintf("%s (%s)", run.Run.AsTime().Format(time.RFC3339), run.State))
	}

	for _, childInstance := range instance.Dependents {
		printStatusTree(childInstance, subtree)
	}
	return tree
}
