package job

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/internal/lib/tree"
	"github.com/odpf/optimus/models"
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
// TODO: Simplify later after the restructure,
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
type priorityResolver struct{}

// NewPriorityResolver create an instance of priorityResolver
func NewPriorityResolver() *priorityResolver {
	return &priorityResolver{}
}

// Resolve takes jobSpecs and returns them with resolved priorities
func (a *priorityResolver) Resolve(_ context.Context, jobSpecs []models.JobSpec,
	progressObserver progress.Observer) ([]models.JobSpec, error) {
	if err := a.resolvePriorities(jobSpecs, progressObserver); err != nil {
		return nil, fmt.Errorf("error occurred while resolving priority: %w", err)
	}
	notify(progressObserver, &models.ProgressJobPriorityWeightAssign{})
	return jobSpecs, nil
}

// resolvePriorities resolves priorities of all provided jobs
func (a *priorityResolver) resolvePriorities(jobSpecs []models.JobSpec, progressObserver progress.Observer) error {
	// build a multi-root tree from all jobs based on their dependencies
	multiRootTree, err := a.buildMultiRootDependencyTree(jobSpecs, progressObserver)
	if err != nil {
		return err
	}

	// perform a breadth first traversal on all trees and assign weights.
	// higher weights are assign to the nodes on the top, and the weight
	// reduces as we go down the tree level
	taskPriorityMap := map[string]int{}
	a.assignWeight(multiRootTree.GetRootNodes(), MaxPriorityWeight, taskPriorityMap)

	for idx, jobSpec := range jobSpecs {
		priority, ok := taskPriorityMap[jobSpec.Name]
		if !ok {
			return fmt.Errorf("%s: %w", jobSpec.Name, ErrPriorityNotFound)
		}
		jobSpec.Task.Priority = priority
		jobSpecs[idx] = jobSpec
	}

	return nil
}

func (a *priorityResolver) assignWeight(rootNodes []*tree.TreeNode, weight int, taskPriorityMap map[string]int) {
	subChildren := []*tree.TreeNode{}
	for _, rootNode := range rootNodes {
		taskPriorityMap[rootNode.GetName()] = weight
		subChildren = append(subChildren, rootNode.Dependents...)
	}
	if len(subChildren) > 0 {
		a.assignWeight(subChildren, weight-PriorityWeightGap, taskPriorityMap)
	}
}

// buildMultiRootDependencyTree - converts []JobSpec into a MultiRootTree
// based on the dependencies of each DAG.
func (a *priorityResolver) buildMultiRootDependencyTree(jobSpecs []models.JobSpec, progressObserver progress.Observer) (*tree.MultiRootTree, error) {
	// creates map[jobName]jobSpec for faster retrieval
	jobSpecMap := make(map[string]models.JobSpec)
	for _, dagSpec := range jobSpecs {
		jobSpecMap[dagSpec.Name] = dagSpec
	}

	// build a multi root tree and assign dependencies
	// ignore any other dependency apart from intra-tenant
	multiRootTree := tree.NewMultiRootTree()
	for _, childSpec := range jobSpecMap {
		childNode := a.findOrCreateDAGNode(multiRootTree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			missingParent := false
			parentSpec, ok := jobSpecMap[depDAG.Job.Name]
			if !ok {
				if depDAG.Type == models.JobSpecDependencyTypeIntra {
					// if its intra dependency, ideally this should not happen but instead of failing
					// its better to simply soft fail by notifying about this action
					// this will cause us to treat it as a dummy job with a unique root
					notify(progressObserver, &models.ProgressJobPriorityWeightAssignmentFailed{Err: fmt.Errorf("%s: %w", depDAG.Job.Name, ErrJobSpecNotFound)})
				}

				// when the dependency of a jobSpec belong to some other tenant or is external, the jobSpec won't
				// be available in jobSpecs []models.JobSpec object (which is tenant specific)
				// so we'll add a dummy JobSpec for that cross tenant/external dependency.
				parentSpec = models.JobSpec{Name: depDAG.Job.Name, Dependencies: make(map[string]models.JobSpecDependency)}
				missingParent = true
			}
			parentNode := a.findOrCreateDAGNode(multiRootTree, parentSpec)
			parentNode.AddDependent(childNode)
			multiRootTree.AddNode(parentNode)
			if missingParent {
				// dependency that are outside current project will be considered as root because
				// optimus don't know dependencies of those external parents
				multiRootTree.MarkRoot(parentNode)
			}
		}

		// the DAGs with no dependencies are root nodes for the tree
		if len(childSpec.Dependencies) == 0 {
			multiRootTree.MarkRoot(childNode)
		}
	}

	if err := multiRootTree.ValidateCyclic(); err != nil {
		return nil, err
	}
	return multiRootTree, nil
}

func (*priorityResolver) findOrCreateDAGNode(dagTree *tree.MultiRootTree, dagSpec models.JobSpec) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = tree.NewTreeNode(dagSpec)
		dagTree.AddNode(node)
	}
	return node
}

func notify(progressObserver progress.Observer, evt progress.Event) {
	if progressObserver != nil {
		progressObserver.Notify(evt)
	}
}
