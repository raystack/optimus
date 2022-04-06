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
	"github.com/xlab/treeprint"
	"google.golang.org/grpc"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/set"
)

func replayCreateCommand(conf *config.ClientConfig) *cli.Command {
	var (
		dryRun           = false
		forceRun         = false
		ignoreDownstream = false
		allDownstream    = false
		skipConfirm      = false
		namespaceName    string
	)

	reCmd := &cli.Command{
		Use:     "create",
		Short:   "Run replay operation on a dag based on provided start and end date range",
		Example: "optimus replay create <job_name> <2020-02-03> [2020-02-05]",
		Long: `
This operation takes three arguments, first is DAG name[required]
used in optimus specification, second is start date[required] of
replay, third is end date[optional] of replay. 
Date ranges are inclusive.
		`,
		Args: func(cmd *cli.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("job name is required")
			}
			if len(args) < 2 { //nolint: gomnd
				return errors.New("replay start date is required")
			}
			return nil
		},
	}
	reCmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	reCmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "Namespace of job that needs to be replayed")
	reCmd.MarkFlagRequired("namespace")

	reCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", dryRun, "Only do a trial run with no permanent changes")
	reCmd.Flags().BoolVarP(&forceRun, "force", "f", forceRun, "Run replay even if a previous run is in progress")
	reCmd.Flags().BoolVar(&skipConfirm, "confirm", skipConfirm, "Skip asking for confirmation")
	reCmd.Flags().BoolVar(&ignoreDownstream, "ignore-downstream", ignoreDownstream, "Run without replaying downstream jobs")
	reCmd.Flags().BoolVar(&allDownstream, "all-downstream", allDownstream, "Run replay for all downstream across namespaces")

	reCmd.RunE = func(cmd *cli.Command, args []string) error {
		projectName := conf.Project.Name
		l := initClientLogger(conf.Log)
		endDate := args[1]
		if len(args) >= 3 { //nolint: gomnd
			endDate = args[2]
		}

		var allowedDownstreamNamespaces []string
		if !ignoreDownstream {
			if allDownstream {
				allowedDownstreamNamespaces = []string{"*"}
			} else {
				allowedDownstreamNamespaces = []string{namespaceName}
			}
		}

		if err := printReplayExecutionTree(l, projectName, namespaceName, args[0], args[1], endDate, allowedDownstreamNamespaces, conf.Host); err != nil {
			return err
		}
		if dryRun {
			// if only dry run, exit now
			return nil
		}

		if !skipConfirm {
			proceedWithReplay := AnswerYes
			if err := survey.AskOne(&survey.Select{
				Message: "Proceed with replay?",
				Options: []string{AnswerYes, AnswerNo},
				Default: AnswerNo,
			}, &proceedWithReplay); err != nil {
				return err
			}
			if proceedWithReplay == AnswerNo {
				l.Info("Aborting...")
				return nil
			}
		}

		replayID, err := runReplayRequest(l, projectName, namespaceName, args[0], args[1], endDate, forceRun,
			allowedDownstreamNamespaces, conf.Host)
		if err != nil {
			return err
		}
		l.Info(coloredSuccess("Replay request created with id %s", replayID))
		return nil
	}
	return reCmd
}

func printReplayExecutionTree(l log.Logger, projectName, namespace, jobName, startDate, endDate string,
	allowedDownstreamNamespaces []string, host string) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
	defer replayRequestCancel()

	replay := pb.NewReplayServiceClient(conn)
	replayRequest := &pb.ReplayDryRunRequest{
		ProjectName:                 projectName,
		JobName:                     jobName,
		NamespaceName:               namespace,
		StartDate:                   startDate,
		EndDate:                     endDate,
		AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
	}

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	replayDryRunResponse, err := replay.ReplayDryRun(replayRequestTimeout, replayRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Replay dry run took too long, timing out"))
		}
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}

	printReplayDryRunResponse(l, replayRequest, replayDryRunResponse)
	return nil
}

func printReplayDryRunResponse(l log.Logger, replayRequest *pb.ReplayDryRunRequest, replayDryRunResponse *pb.ReplayDryRunResponse) {
	l.Info(fmt.Sprintf("For %s project and %s namespace\n", coloredNotice(replayRequest.ProjectName), coloredNotice(replayRequest.NamespaceName)))
	l.Info(coloredNotice("\n> Replay runs"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"Index",
		"Job",
		"Run",
	})
	taskRerunsMap := make(map[string]taskRunBlock)
	formatRunsPerJobInstance(replayDryRunResponse.ExecutionTree, taskRerunsMap, 0)

	// sort run block
	taskRerunsSorted := set.NewTreeSetWith(taskRunBlockComparator)
	for _, block := range taskRerunsMap {
		taskRerunsSorted.Add(block)
	}
	for idx, rawBlock := range taskRerunsSorted.Values() {
		runBlock := rawBlock.(taskRunBlock)
		runTimes := []string{}
		for _, runRaw := range runBlock.runs.Values() {
			runTimes = append(runTimes, runRaw.(time.Time).String())
		}

		table.Append([]string{fmt.Sprintf("%d", idx+1), runBlock.name, strings.Join(runTimes, "\n")})
	}
	table.Render()

	// print tree
	l.Info(coloredNotice("\n> Dependency tree"))
	l.Info(printExecutionTree(replayDryRunResponse.ExecutionTree, treeprint.New()).String())

	// ignored jobs
	if len(replayDryRunResponse.IgnoredJobs) > 0 {
		l.Info("> Ignored jobs")
		ignoredJobsCount := 0
		for _, job := range replayDryRunResponse.IgnoredJobs {
			ignoredJobsCount++
			l.Info(fmt.Sprintf("%d. %s", ignoredJobsCount, job))
		}
		// separator
		l.Info("")
	}
}

// printExecutionTree creates a ascii tree to visually inspect
// instance dependencies that will be recomputed after replay operation
func printExecutionTree(instance *pb.ReplayExecutionTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		if run.AsTime().Before(time.Now()) {
			runBranch.AddNode(run.AsTime().Format(time.RFC3339))
		}
	}

	for _, childInstance := range instance.Dependents {
		printExecutionTree(childInstance, subtree)
	}
	return tree
}

func runReplayRequest(l log.Logger, projectName, namespace, jobName, startDate, endDate string, forceRun bool,
	allowedDownstreamNamespaces []string, host string) (string, error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	conn, err := createConnection(dialTimeoutCtx, host)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return "", err
	}
	defer conn.Close()

	replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
	defer replayRequestCancel()

	l.Info("\n> Initiating replay")
	if forceRun {
		l.Info(coloredNotice("> Force running replay even if its already in progress"))
	}
	replay := pb.NewReplayServiceClient(conn)
	replayRequest := &pb.ReplayRequest{
		ProjectName:                 projectName,
		JobName:                     jobName,
		NamespaceName:               namespace,
		StartDate:                   startDate,
		EndDate:                     endDate,
		Force:                       forceRun,
		AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
	}

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	replayResponse, err := replay.Replay(replayRequestTimeout, replayRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Replay request took too long, timing out"))
		}
		return "", fmt.Errorf("request failed for job %s: %w", jobName, err)
	}
	return replayResponse.Id, nil
}
