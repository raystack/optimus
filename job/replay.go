package job

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/set"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// ReplayDateFormat YYYY-mm-dd for replay dates and dag start date
	ReplayDateFormat = "2006-01-02"
)

func (srv *Service) populateRequestWithJobSpecs(replayRequest *models.ReplayRequest) error {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(replayRequest.Project)
	jobSpecs, err := srv.GetDependencyResolvedSpecs(replayRequest.Project, projectJobSpecRepo, nil)
	if err != nil {
		return err
	}
	jobSpecMap := make(map[string]models.JobSpec)
	for _, currSpec := range jobSpecs {
		jobSpecMap[currSpec.Name] = currSpec
	}
	replayRequest.JobSpecMap = jobSpecMap
	return nil
}

func (srv *Service) ReplayDryRun(replayRequest *models.ReplayRequest) (*tree.TreeNode, error) {
	if err := srv.populateRequestWithJobSpecs(replayRequest); err != nil {
		return nil, err
	}

	rootInstance, err := prepareReplayExecutionTree(replayRequest)
	if err != nil {
		return nil, err
	}

	return rootInstance, nil
}

func (srv *Service) Replay(ctx context.Context, replayRequest *models.ReplayRequest) (string, error) {
	if err := srv.populateRequestWithJobSpecs(replayRequest); err != nil {
		return "", err
	}

	replayUUID, err := srv.replayManager.Replay(ctx, replayRequest)
	if err != nil {
		return "", err
	}
	return replayUUID, nil
}

// prepareReplayExecutionTree creates a execution tree for replay operation
func prepareReplayExecutionTree(replayRequest *models.ReplayRequest) (*tree.TreeNode, error) {
	replayJobSpec, found := replayRequest.JobSpecMap[replayRequest.Job.Name]
	if !found {
		return nil, fmt.Errorf("couldn't find any job with name %s", replayRequest.Job.Name)
	}

	// compute runs that require replay
	dagTree := tree.NewMultiRootTree()
	parentNode := tree.NewTreeNode(replayJobSpec)
	if runs, err := getRunsBetweenDates(replayRequest.Start, replayRequest.End, replayJobSpec.Schedule.Interval); err == nil {
		for _, run := range runs {
			parentNode.Runs.Add(run)
		}
	} else {
		return nil, err
	}
	dagTree.AddNode(parentNode)

	rootInstance, err := populateDownstreamDAGs(dagTree, replayJobSpec, replayRequest.JobSpecMap)
	if err != nil {
		return nil, err
	}

	rootInstance, err = populateDownstreamRuns(rootInstance)
	if err != nil {
		return nil, err
	}

	return rootInstance, nil
}

func findOrCreateDAGNode(dagTree *tree.MultiRootTree, dagSpec models.JobSpec) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = tree.NewTreeNode(dagSpec)
		dagTree.AddNode(node)
	}
	return node
}

func populateDownstreamDAGs(dagTree *tree.MultiRootTree, jobSpec models.JobSpec, jobSpecMap map[string]models.JobSpec) (*tree.TreeNode, error) {
	for _, childSpec := range jobSpecMap {
		childNode := findOrCreateDAGNode(dagTree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			var isExternal = false
			parentSpec, ok := jobSpecMap[depDAG.Job.Name]
			if !ok {
				if depDAG.Type == models.JobSpecDependencyTypeIntra {
					return nil, errors.Wrap(ErrJobSpecNotFound, depDAG.Job.Name)
				}
				// when the dependency of a jobSpec belong to some other tenant or is external, the jobSpec won't
				// be available in jobSpecs []models.JobSpec object (which is tenant specific)
				// so we'll add a dummy JobSpec for that cross tenant/external dependency.
				parentSpec = models.JobSpec{Name: depDAG.Job.Name, Dependencies: make(map[string]models.JobSpecDependency)}
				isExternal = true
			}
			parentNode := findOrCreateDAGNode(dagTree, parentSpec)
			parentNode.AddDependent(childNode)
			dagTree.AddNode(parentNode)

			if isExternal {
				// dependency that are outside current project will be considered as root because
				// optimus don't know dependencies of those external parents
				dagTree.MarkRoot(parentNode)
			}
		}

		if len(childSpec.Dependencies) == 0 {
			dagTree.MarkRoot(childNode)
		}
	}

	if err := dagTree.IsCyclic(); err != nil {
		return nil, err
	}

	// since we are adding the rootNode at start, it will always be present
	rootNode, _ := dagTree.GetNodeByName(jobSpec.Name)

	return rootNode, nil
}

func populateDownstreamRuns(parentNode *tree.TreeNode) (*tree.TreeNode, error) {
	for idx, childNode := range parentNode.Dependents {
		childDag := childNode.Data.(models.JobSpec)
		taskSchedule, err := cron.ParseCronSchedule(childDag.Schedule.Interval)
		if err != nil {
			return nil, err
		}

		for _, parentRunDateRaw := range parentNode.Runs.Values() { //
			parentRunDate := parentRunDateRaw.(time.Time)

			// subtract 1 day to make end inclusive
			parentEndDate := parentRunDate.Add(time.Hour * -24).Add(childDag.Task.Window.Size)

			// subtracting 1 sec to accommodate next call of cron
			// where parent task and current task has same scheduled interval
			taskFirstEffectedRun := taskSchedule.Next(parentRunDate.Add(-1 * time.Second))

			//make sure it is after current dag start date
			if taskFirstEffectedRun.Before(childDag.Schedule.StartDate) {
				continue
			}

			runs, err := getRunsBetweenDates(parentRunDate, parentEndDate, childDag.Schedule.Interval)
			if err != nil {
				return nil, errors.Wrap(err, "failed to find runs with parent dag")
			}
			for _, run := range runs {
				childNode.Runs.Add(run)
			}
		}
		updatedChildNode, err := populateDownstreamRuns(childNode)
		if err != nil {
			return nil, err
		}
		parentNode.Dependents[idx] = updatedChildNode
	}
	return parentNode, nil
}

// getRunsBetweenDates provides execution runs from start to end following a schedule interval
// start and end both are inclusive
func getRunsBetweenDates(start time.Time, end time.Time, schedule string) ([]time.Time, error) {
	var runs []time.Time

	// standard cron parser without descriptors
	schd, err := cron.ParseCronSchedule(schedule)
	if err != nil {
		return nil, err
	}

	replayRunEnd := schd.Next(end)
	for replayRunEnd.Before(end.AddDate(0, 0, 1)) {
		replayRunEnd = schd.Next(replayRunEnd)
	}

	// loop until start date reaches end date
	for run := schd.Next(start.Add(time.Second * -1)); run.Before(replayRunEnd); run = schd.Next(run) {
		runs = append(runs, run)
	}

	return runs, nil
}

func (srv *Service) GetStatus(ctx context.Context, replayRequest *models.ReplayRequest) (*models.ReplayState, error) {
	// Get replay
	replaySpec, err := srv.replayManager.GetReplay(replayRequest.ID)
	if err != nil {
		return nil, err
	}

	// populate
	replayRequest.Start = replaySpec.StartDate
	replayRequest.End = replaySpec.EndDate
	replayRequest.Job = replaySpec.Job
	if err = srv.populateRequestWithJobSpecs(replayRequest); err != nil {
		return nil, err
	}

	// forming tree with status per run
	rootInstance, err := srv.prepareReplayStatusTree(ctx, replayRequest)
	if err != nil {
		return nil, err
	}

	return &models.ReplayState{
		Status: replaySpec.Status,
		Node:   rootInstance,
	}, nil
}

func TimeOfJobStatusComparator(a, b interface{}) int {
	aAsserted := a.(models.JobStatus).ScheduledAt
	bAsserted := b.(models.JobStatus).ScheduledAt
	switch {
	case aAsserted.After(bAsserted):
		return 1
	case aAsserted.Before(bAsserted):
		return -1
	default:
		return 0
	}
}

// prepareReplayStatusTree creates a execution tree with the status per run
func (srv *Service) prepareReplayStatusTree(ctx context.Context, replayRequest *models.ReplayRequest) (*tree.TreeNode, error) {
	replayJobSpec, found := replayRequest.JobSpecMap[replayRequest.Job.Name]
	if !found {
		return nil, fmt.Errorf("couldn't find any job with name %s", replayRequest.Job.Name)
	}

	// compute runs that require replay
	dagTree := tree.NewMultiRootTree()
	parentNode := tree.NewTreeNode(replayJobSpec)
	jobStatusList, err := srv.replayManager.GetRunStatus(ctx, replayRequest, replayRequest.Job.Name)
	if err != nil {
		return nil, err
	}
	parentNode.Runs = set.NewTreeSetWith(TimeOfJobStatusComparator)
	for _, jobStatus := range jobStatusList {
		parentNode.Runs.Add(jobStatus)
	}
	dagTree.AddNode(parentNode)

	rootInstance, err := populateDownstreamDAGs(dagTree, replayJobSpec, replayRequest.JobSpecMap)
	if err != nil {
		return nil, err
	}

	rootInstance, err = srv.populateDownstreamRunsWithStatus(ctx, rootInstance, replayRequest)
	if err != nil {
		return nil, err
	}

	return rootInstance, nil
}

func (srv *Service) populateDownstreamRunsWithStatus(ctx context.Context, parentNode *tree.TreeNode, replayRequest *models.ReplayRequest) (*tree.TreeNode, error) {
	for idx, childNode := range parentNode.Dependents {
		childDag := childNode.Data.(models.JobSpec)

		jobStatusList, err := srv.replayManager.GetRunStatus(ctx, replayRequest, childDag.Name)
		if err != nil {
			return nil, err
		}
		childNode.Runs = set.NewTreeSetWith(TimeOfJobStatusComparator)
		for _, jobStatus := range jobStatusList {
			childNode.Runs.Add(jobStatus)
		}

		updatedChildNode, err := srv.populateDownstreamRunsWithStatus(ctx, childNode, replayRequest)
		if err != nil {
			return nil, err
		}
		parentNode.Dependents[idx] = updatedChildNode
	}
	return parentNode, nil
}
