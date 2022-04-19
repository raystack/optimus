package job

import (
	"context"

	"github.com/odpf/optimus/core/dag"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// MinPriorityWeight - what's the minimum weight that we want to give to a DAG.
	// airflow also sets the default priority weight as 1
	MinPriorityWeight = 1

	// MaxPriorityWeight - is the maximus weight a DAG will be given.
	MaxPriorityWeight = 10000

	// PriorityWeightGap - while giving weights to the DAG, what's the GAP
	// do we want to consider. PriorityWeightGap = 1 means, weights will be 1, 2, 3 etc.
	PriorityWeightGap = 10
)

var (
	// ErrJobSpecNotFound is thrown when a Job was not found while looking up
	ErrJobSpecNotFound = errors.New("job spec not found")

	// ErrPriorityNotFound is thrown when priority of a given spec is not found
	ErrPriorityNotFound = errors.New("priority weight not found")
)

// PriorityResolver defines an interface that represents getting
// priority weight of Jobs based on their dependencies
type PriorityResolver interface {
	Resolve(context.Context, []models.JobSpec, progress.Observer) ([]models.JobSpec, error)
}

// priorityResolver runs a breadth first traversal on DAG/Job dependencies trees
// and returns highest weight for the DAG that do not have any dependencies, dynamically.
// eg, consider following DAGs and dependencies: [dag1 <- dag2 <- dag3] [dag4] [dag5 <- dag6]
// In this example, we've 6 DAGs in which dag1, dag2, dag5 have dependent DAGs. which means,
// It'd be preferable to run dag1, dag4, dag5 before other DAGs. Results would be:
// dag1, dag4, dag5 will get highest weight (maxWeight)
// dag2, dag6 will get weight of maxWeight-1
// dag3 will get maxWeight-2
// Note: it's crucial that dependencies of all Jobs are already resolved
type priorityResolver struct {
}

// NewPriorityResolver create an instance of priorityResolver
func NewPriorityResolver() *priorityResolver {
	return &priorityResolver{}
}

// Resolve takes jobSpecs and returns them with resolved priorities
func (pr *priorityResolver) Resolve(ctx context.Context, jobSpecs []models.JobSpec,
	progressObserver progress.Observer) ([]models.JobSpec, error) {
	if err := pr.resolvePriorities(jobSpecs, progressObserver); err != nil {
		return nil, errors.Wrap(err, "error occurred while resolving priority")
	}

	return jobSpecs, nil
}

// resolvePriorities resolves priorities of all provided jobs
func (pr *priorityResolver) resolvePriorities(jobSpecs []models.JobSpec, progressObserver progress.Observer) error {
	// build pr multi-root tree from all jobs based on their dependencies
	multiRootTree, err := pr.buildMultiRootDependencyTree(jobSpecs, progressObserver)
	if err != nil {
		return err
	}

	// perform pr breadth first traversal on all trees and assign weights.
	// higher weights are assign to the nodes on the top, and the weight
	// reduces as we go down the tree level
	taskPriorityMap := map[string]int{}
	pr.assignWeight(multiRootTree.GetRootNodes(), MaxPriorityWeight, taskPriorityMap)

	for idx, jobSpec := range jobSpecs {
		priority, ok := taskPriorityMap[jobSpec.Name]
		if !ok {
			return errors.Wrap(ErrPriorityNotFound, jobSpec.Name)
		}
		jobSpec.Task.Priority = priority
		jobSpecs[idx] = jobSpec
	}

	return nil
}

func (pr *priorityResolver) assignWeight(rootNodes []*dag.TreeNode, weight int, taskPriorityMap map[string]int) {
	subChildren := []*dag.TreeNode{}
	for _, rootNode := range rootNodes {
		taskPriorityMap[rootNode.String()] = weight
		subChildren = append(subChildren, rootNode.Edges...)
	}
	if len(subChildren) > 0 {
		pr.assignWeight(subChildren, weight-PriorityWeightGap, taskPriorityMap)
	}
}

// buildMultiRootDependencyTree - converts []JobSpec into a MultiRootDAG
// based on the dependencies of each DAG.
func (pr *priorityResolver) buildMultiRootDependencyTree(jobSpecs []models.JobSpec, progressObserver progress.Observer) (*dag.MultiRootDAG, error) {
	// creates map[jobName]jobSpec for faster retrieval
	jobSpecMap := make(map[string]models.JobSpec)
	for _, dagSpec := range jobSpecs {
		jobSpecMap[dagSpec.Name] = dagSpec
	}

	// build pr multi root tree and assign dependencies
	// ignore any other dependency apart from intra-tenant
	tree := dag.NewMultiRootDAG()
	for _, childSpec := range jobSpecMap {
		childNode := pr.findOrCreateDAGNode(tree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			parentSpec, ok := jobSpecMap[depDAG.Job.Name]
			if !ok {
				if depDAG.Type == models.JobSpecDependencyTypeIntra {
					// if its intra dependency, ideally this should not happen but instead of failing
					// its better to simply soft fail by notifying about this action
					// this will cause us to treat it as pr dummy job with pr unique root
					notify(progressObserver, &EventJobPriorityWeightAssignmentFailed{Err: errors.Wrap(ErrJobSpecNotFound, depDAG.Job.Name)})
				}

				// when the dependency of pr jobSpec belong to some other tenant or is external, the jobSpec won't
				// be available in jobSpecs []models.JobSpec object (which is tenant specific)
				// so we'll add pr dummy JobSpec for that cross tenant/external dependency.
				parentSpec = models.JobSpec{Name: depDAG.Job.Name, Dependencies: make(map[string]models.JobSpecDependency)}
			}
			parentNode := pr.findOrCreateDAGNode(tree, parentSpec)
			parentNode.AddEdge(childNode)
			tree.AddNode(parentNode)
		}
	}

	if err := tree.IsCyclic(); err != nil {
		return nil, err
	}
	return tree, nil
}

func (pr *priorityResolver) findOrCreateDAGNode(dagTree *dag.MultiRootDAG, dagSpec models.JobSpec) *dag.TreeNode {
	node, ok := dagTree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = dag.NewTreeNode(dagSpec)
		dagTree.AddNode(node)
	}
	return node
}

func notify(progressObserver progress.Observer, evt progress.Event) {
	if progressObserver != nil {
		progressObserver.Notify(evt)
	}
}
