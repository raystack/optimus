package resolver

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/lib/tree"
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
	// ErrPriorityNotFound is thrown when priority of a given spec is not found
	ErrPriorityNotFound = errors.New("priority weight not found")
)

// PriorityResolver runs a breadth first traversal on DAG/Job dependencies trees
// and returns highest weight for the DAG that do not have any dependencies, dynamically.
// eg, consider following DAGs and dependencies: [dag1 <- dag2 <- dag3] [dag4] [dag5 <- dag6]
// In this example, we've 6 DAGs in which dag1, dag2, dag5 have dependent DAGs. which means,
// It'd be preferable to run dag1, dag4, dag5 before other DAGs. Results would be:
// dag1, dag4, dag5 will get highest weight (maxWeight)
// dag2, dag6 will get weight of maxWeight-1
// dag3 will get maxWeight-2
// Note: it's crucial that dependencies of all Jobs are already resolved
type PriorityResolver struct{}

// NewPriorityResolver create an instance of PriorityResolver
func NewPriorityResolver() *PriorityResolver {
	return &PriorityResolver{}
}

// Resolve takes jobsWithDetails and returns them with resolved priorities
func (a *PriorityResolver) Resolve(_ context.Context, jobWithDetails []*scheduler.JobWithDetails) error {
	if err := a.resolvePriorities(jobWithDetails); err != nil {
		return fmt.Errorf("error occurred while resolving priority: %w", err)
	}
	return nil
}

// resolvePriorities resolves priorities of all provided jobs
func (a *PriorityResolver) resolvePriorities(jobsWithDetails []*scheduler.JobWithDetails) error {
	// build a multi-root tree from all jobs based on their dependencies
	multiRootTree, err := a.buildMultiRootDependencyTree(jobsWithDetails)
	if err != nil {
		return err
	}

	// perform a breadth first traversal on all trees and assign weights.
	// higher weights are assign to the nodes on the top, and the weight
	// reduces as we go down the tree level
	taskPriorityMap := map[string]int{}
	a.assignWeight(multiRootTree.GetRootNodes(), MaxPriorityWeight, taskPriorityMap)

	for _, jobWithDetails := range jobsWithDetails {
		priority, ok := taskPriorityMap[jobWithDetails.Name.String()]
		if !ok {
			return fmt.Errorf("%s: %w", jobWithDetails.Name, ErrPriorityNotFound)
		}
		jobWithDetails.Priority = priority
	}

	return nil
}

func (a *PriorityResolver) assignWeight(rootNodes []*tree.TreeNode, weight int, taskPriorityMap map[string]int) {
	subChildren := []*tree.TreeNode{}
	for _, rootNode := range rootNodes {
		taskPriorityMap[rootNode.GetName()] = weight
		subChildren = append(subChildren, rootNode.Dependents...)
	}
	if len(subChildren) > 0 {
		a.assignWeight(subChildren, weight-PriorityWeightGap, taskPriorityMap)
	}
}

// buildMultiRootDependencyTree - converts []jobWithDetails into a MultiRootTree
// based on the dependencies of each DAG.
func (a *PriorityResolver) buildMultiRootDependencyTree(jobsWithDetails []*scheduler.JobWithDetails) (*tree.MultiRootTree, error) {
	// creates map[jobName]jobWithDetails for faster retrieval
	jobWithDetailsMap := make(map[string]*scheduler.JobWithDetails)
	for _, dagSpec := range jobsWithDetails {
		jobWithDetailsMap[dagSpec.Name.String()] = dagSpec
	}

	// build a multi root tree and assign dependencies
	// ignore any other dependency apart from intra-tenant
	multiRootTree := tree.NewMultiRootTree()
	for _, childSpec := range jobWithDetailsMap {
		childNode := a.findOrCreateDAGNode(multiRootTree, childSpec)
		for _, upstream := range childSpec.Upstreams.UpstreamJobs {
			missingParent := false
			parentSpec, ok := jobWithDetailsMap[upstream.JobName]
			if !ok {
				// when the dependency of a jobWithDetails belong to some other tenant or is external, the jobWithDetails won't
				// be available in jobsWithDetails []job_run.jobWithDetails object (which is tenant specific)
				// so we'll add a dummy jobWithDetails for that cross tenant/external dependency.
				parentSpec = &scheduler.JobWithDetails{
					Name:      scheduler.JobName(upstream.JobName),
					Upstreams: scheduler.Upstreams{},
				}
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
		if len(childSpec.Upstreams.UpstreamJobs) == 0 {
			multiRootTree.MarkRoot(childNode)
		}
	}

	if err := multiRootTree.ValidateCyclic(); err != nil {
		return nil, err
	}
	return multiRootTree, nil
}

func (*PriorityResolver) findOrCreateDAGNode(dagTree *tree.MultiRootTree, jobDetails *scheduler.JobWithDetails) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(jobDetails.Name.String())
	if !ok {
		node = tree.NewTreeNode(jobDetails)
		dagTree.AddNode(node)
	}
	return node
}
