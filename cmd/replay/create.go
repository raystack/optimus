package replay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/xlab/treeprint"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/namespace"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/set"
)

type taskRunBlock struct {
	name   string
	height int
	runs   set.Set
}

type createCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	survey *survey.ReplayCreateSurvey

	dryRun           bool
	forceRun         bool
	ignoreDownstream bool
	allDownstream    bool
	skipConfirm      bool
	namespaceName    string
}

// NewCreateCommand initializes command for replay create
func NewCreateCommand(clientConfig *config.ClientConfig) *cobra.Command {
	create := &createCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Run replay operation on a dag based on provided start and end date range",
		Example: "optimus replay create <job_name> <2020-02-03> [2020-02-05]",
		Long: `
This operation takes three arguments, first is DAG name[required]
used in optimus specification, second is start date[required] of
replay, third is end date[optional] of replay. 
Date ranges are inclusive.
		`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("job name is required")
			}
			if len(args) < 2 { //nolint: gomnd
				return errors.New("replay start date is required")
			}
			return nil
		},
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	cmd.Flags().StringVarP(&create.namespaceName, "namespace", "n", create.namespaceName, "Namespace of job that needs to be replayed")
	cmd.MarkFlagRequired("namespace")

	cmd.Flags().BoolVarP(&create.dryRun, "dry-run", "d", create.dryRun, "Only do a trial run with no permanent changes")
	cmd.Flags().BoolVarP(&create.forceRun, "force", "f", create.forceRun, "Run replay even if a previous run is in progress")
	cmd.Flags().BoolVar(&create.skipConfirm, "confirm", create.skipConfirm, "Skip asking for confirmation")
	cmd.Flags().BoolVar(&create.ignoreDownstream, "ignore-downstream", create.ignoreDownstream, "Run without replaying downstream jobs")
	cmd.Flags().BoolVar(&create.allDownstream, "all-downstream", create.allDownstream, "Run replay for all downstream across namespaces")
	return cmd
}

func (c *createCommand) PreRunE(_ *cobra.Command, _ []string) error {
	c.logger = logger.NewClientLogger(c.clientConfig.Log)
	c.survey = survey.NewReplayCreateSurvey(c.logger)
	return nil
}

func (c *createCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	startDate := args[1]
	endDate := args[1]
	if len(args) >= 3 { //nolint: gomnd
		endDate = args[2]
	}

	if err := c.printReplayExecutionTree(jobName, startDate, endDate); err != nil {
		return err
	}
	if c.dryRun {
		// if only dry run, exit now
		return nil
	}

	if !c.skipConfirm {
		confirm, err := c.survey.AskConfirmToContinue()
		if err != nil {
			return err
		}
		if !confirm {
			return nil
		}
	}

	replayID, err := c.runReplayRequest(jobName, startDate, endDate)
	if err != nil {
		return err
	}
	c.logger.Info(logger.ColoredSuccess("Replay request created with id %s", replayID))
	return nil
}

func (c *createCommand) getAllowedDownstreamNamespaces() []string {
	var allowedDownstreamNamespaces []string
	if !c.ignoreDownstream {
		allowedDownstreamNamespaces = namespace.GetAllowedDownstreamNamespaces(c.namespaceName, c.allDownstream)
	}
	return allowedDownstreamNamespaces
}

func (c *createCommand) runReplayRequest(jobName, startDate, endDate string) (string, error) {
	conn, err := connectivity.NewConnectivity(c.clientConfig.Host, replayTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	c.logger.Info("\n> Initiating replay")
	if c.forceRun {
		c.logger.Info(logger.ColoredNotice("> Force running replay even if its already in progress"))
	}

	replay := pb.NewReplayServiceClient(conn.GetConnection())
	replayRequest := &pb.ReplayRequest{
		ProjectName:                 c.clientConfig.Project.Name,
		JobName:                     jobName,
		NamespaceName:               c.namespaceName,
		StartDate:                   startDate,
		EndDate:                     endDate,
		Force:                       c.forceRun,
		AllowedDownstreamNamespaces: c.getAllowedDownstreamNamespaces(),
	}

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	replayResponse, err := replay.Replay(conn.GetContext(), replayRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error(logger.ColoredError("Replay request took too long, timing out"))
		}
		return "", fmt.Errorf("request failed for job %s: %w", jobName, err)
	}
	return replayResponse.Id, nil
}

func (c *createCommand) printReplayExecutionTree(jobName, startDate, endDate string) error {
	conn, err := connectivity.NewConnectivity(c.clientConfig.Host, replayTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	replay := pb.NewReplayServiceClient(conn.GetConnection())
	replayRequest := &pb.ReplayDryRunRequest{
		ProjectName:                 c.clientConfig.Project.Name,
		JobName:                     jobName,
		NamespaceName:               c.namespaceName,
		StartDate:                   startDate,
		EndDate:                     endDate,
		AllowedDownstreamNamespaces: c.getAllowedDownstreamNamespaces(),
	}

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	replayDryRunResponse, err := replay.ReplayDryRun(conn.GetContext(), replayRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error(logger.ColoredError("Replay dry run took too long, timing out"))
		}
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}

	c.printReplayDryRunResponse(replayRequest, replayDryRunResponse)
	return nil
}

func (c *createCommand) printReplayDryRunResponse(replayRequest *pb.ReplayDryRunRequest, replayDryRunResponse *pb.ReplayDryRunResponse) {
	c.logger.Info(fmt.Sprintf("For %s project and %s namespace\n", logger.ColoredNotice(replayRequest.ProjectName), logger.ColoredNotice(replayRequest.NamespaceName)))
	c.logger.Info(logger.ColoredNotice("\n> Replay runs"))
	table := tablewriter.NewWriter(c.logger.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"Index",
		"Job",
		"Run",
	})
	taskRerunsMap := make(map[string]taskRunBlock)
	c.formatRunsPerJobInstance(replayDryRunResponse.ExecutionTree, taskRerunsMap, 0)

	// sort run block
	taskRerunsSorted := set.NewTreeSetWith(c.taskRunBlockComparator)
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
	c.logger.Info(logger.ColoredNotice("\n> Dependency tree"))
	c.logger.Info(c.printExecutionTree(replayDryRunResponse.ExecutionTree, treeprint.New()).String())

	// ignored jobs
	if len(replayDryRunResponse.IgnoredJobs) > 0 {
		c.logger.Info("> Ignored jobs")
		ignoredJobsCount := 0
		for _, job := range replayDryRunResponse.IgnoredJobs {
			ignoredJobsCount++
			c.logger.Info(fmt.Sprintf("%d. %s", ignoredJobsCount, job))
		}
		// separator
		c.logger.Info("")
	}
}

// printExecutionTree creates a ascii tree to visually inspect
// instance dependencies that will be recomputed after replay operation
func (c *createCommand) printExecutionTree(instance *pb.ReplayExecutionTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		if run.AsTime().Before(time.Now()) {
			runBranch.AddNode(run.AsTime().Format(time.RFC3339))
		}
	}

	for _, childInstance := range instance.Dependents {
		c.printExecutionTree(childInstance, subtree)
	}
	return tree
}

func (*createCommand) taskRunBlockComparator(a, b interface{}) int {
	aAsserted := a.(taskRunBlock)
	bAsserted := b.(taskRunBlock)
	switch {
	case aAsserted.height < bAsserted.height:
		return -1
	case aAsserted.height > bAsserted.height:
		return 1
	}
	return strings.Compare(aAsserted.name, bAsserted.name)
}

// formatRunsPerJobInstance returns a hashmap with Job -> Runs[] mapping
func (c *createCommand) formatRunsPerJobInstance(instance *pb.ReplayExecutionTreeNode, taskReruns map[string]taskRunBlock, height int) {
	if _, ok := taskReruns[instance.JobName]; !ok {
		taskReruns[instance.JobName] = taskRunBlock{
			name:   instance.JobName,
			height: height,
			runs:   set.NewTreeSetWithTimeComparator(),
		}
	}

	for _, taskRun := range instance.Runs {
		taskReruns[instance.JobName].runs.Add(taskRun.AsTime())
	}
	for _, child := range instance.Dependents {
		c.formatRunsPerJobInstance(child, taskReruns, height+1)
	}
}
