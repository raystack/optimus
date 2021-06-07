package job

import (
	"fmt"
	"time"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/core/multi_root_tree"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// ReplayDateFormat YYYY-mm-dd for replay dates and dag start date
	ReplayDateFormat = "2006-01-02"
)

func (srv *Service) Replay(namespace models.NamespaceSpec, replayJobSpec models.JobSpec, dryRun bool, start, end time.Time) (*multi_root_tree.TreeNode, error) {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(namespace.ProjectSpec)
	jobSpecs, err := srv.getDependencyResolvedSpecs(namespace.ProjectSpec, projectJobSpecRepo, nil)
	if err != nil {
		return nil, err
	}
	dagSpecMap := make(map[string]models.JobSpec)
	for _, currSpec := range jobSpecs {
		dagSpecMap[currSpec.Name] = currSpec
	}

	rootInstance, err := prepareTree(dagSpecMap, replayJobSpec.Name, start, end)
	if err != nil {
		return nil, err
	}

	if dryRun {
		//if only dry run, exit now
		return rootInstance, err
	}

	return rootInstance, nil
}

// prepareTree creates a execution tree for replay operation
func prepareTree(dagSpecMap map[string]models.JobSpec, replayJobName string, start, end time.Time) (*multi_root_tree.TreeNode, error) {
	replayJobSpec, found := dagSpecMap[replayJobName]
	if !found {
		return nil, fmt.Errorf("couldn't find any job with name %s", replayJobName)
	}

	// compute runs that require replay
	tree := multi_root_tree.NewMultiRootDAGTree()
	parentNode := multi_root_tree.NewTreeNode(replayJobSpec)
	if runs, err := getRunsBetweenDates(start, end, replayJobSpec.Schedule.Interval); err == nil {
		for _, run := range runs {
			parentNode.Runs.Add(run)
		}
	} else {
		return nil, err
	}
	tree.AddNode(parentNode)

	rootInstance, err := populateDownstreamDAGs(tree, replayJobSpec, dagSpecMap)
	if err != nil {
		return nil, err
	}

	rootInstance, err = populateDownstreamRuns(rootInstance)
	if err != nil {
		return nil, err
	}

	return rootInstance, nil
}

func findOrCreateDAGNode(tree *multi_root_tree.MultiRootDAGTree, dagSpec models.JobSpec) *multi_root_tree.TreeNode {
	node, ok := tree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = multi_root_tree.NewTreeNode(dagSpec)
		tree.AddNode(node)
	}
	return node
}

func populateDownstreamDAGs(tree *multi_root_tree.MultiRootDAGTree, jobSpec models.JobSpec, dagSpecMap map[string]models.JobSpec) (*multi_root_tree.TreeNode, error) {
	for _, childSpec := range dagSpecMap {
		childNode := findOrCreateDAGNode(tree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			var isExternal = false
			parentSpec, ok := dagSpecMap[depDAG.Job.Name]
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
			parentNode := findOrCreateDAGNode(tree, parentSpec)
			parentNode.AddDependent(childNode)
			tree.AddNode(parentNode)

			if isExternal {
				// dependency that are outside current project will be considered as root because
				// optimus don't know dependencies of those external parents
				tree.MarkRoot(parentNode)
			}
		}

		if len(childSpec.Dependencies) == 0 {
			tree.MarkRoot(childNode)
		}
	}

	if err := tree.IsCyclic(); err != nil {
		return nil, err
	}

	// since we are adding the rootNode at start, it will always be present
	rootNode, _ := tree.GetNodeByName(jobSpec.Name)

	return rootNode, nil
}

func populateDownstreamRuns(parentNode *multi_root_tree.TreeNode) (*multi_root_tree.TreeNode, error) {
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
