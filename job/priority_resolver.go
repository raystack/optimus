package job

import (
	"github.com/pkg/errors"
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

	// ErrCyclicDependencyEncountered is triggered a tree has a cyclic dependency
	ErrCyclicDependencyEncountered = errors.New("a cycle dependency encountered in the tree")
)

// PriorityResolver defines an interface that represents getting
// priority weight of Jobs based on their dependencies
type PriorityResolver interface {
	Resolve([]models.JobSpec) ([]models.JobSpec, error)
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
func (a *priorityResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	if err := a.resolvePriorities(jobSpecs); err != nil {
		return nil, errors.Wrap(err, "error occurred while resolving priority")
	}

	return jobSpecs, nil
}

// resolvePriorities resolves priorities of all provided jobs
func (a *priorityResolver) resolvePriorities(jobSpecs []models.JobSpec) error {
	// build a multi-root tree from all jobs based on their dependencies
	multiRootTree, err := a.buildMultiRootDependencyTree(jobSpecs)
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
			return errors.Wrap(ErrPriorityNotFound, jobSpec.Name)
		}
		jobSpec.Task.Priority = priority
		jobSpecs[idx] = jobSpec
	}

	return nil
}

func (a *priorityResolver) assignWeight(rootNodes []*DAGNode, weight int, taskPriorityMap map[string]int) {
	subChildren := []*DAGNode{}
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
func (a *priorityResolver) buildMultiRootDependencyTree(jobSpecs []models.JobSpec) (*MultiRootDAGTree, error) {
	// creates map[jobName]jobSpec for faster retrieval
	dagSpecMap := make(map[string]models.JobSpec)
	for _, dagSpec := range jobSpecs {
		dagSpecMap[dagSpec.Name] = dagSpec
	}

	// build a multi root tree and assign dependencies
	tree := NewMultiRootDAGTree()
	for _, childSpec := range dagSpecMap {
		childNode := a.findOrCreateDAGNode(tree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			parentSpec, ok := dagSpecMap[depDAG.Job.Name]
			if !ok {
				return nil, errors.Wrap(ErrJobSpecNotFound, depDAG.Job.Name)
			}
			parentNode := a.findOrCreateDAGNode(tree, parentSpec)
			parentNode.AddDependent(childNode)
			tree.AddNode(parentNode)
		}

		// the DAGs with no dependencies are root nodes for the tree
		if len(childSpec.Dependencies) == 0 {
			tree.SetRoot(childNode)
		}
	}

	if err := tree.IsCyclic(); err != nil {
		return nil, err
	}
	return tree, nil
}

func (a *priorityResolver) findOrCreateDAGNode(tree *MultiRootDAGTree, dagSpec models.JobSpec) *DAGNode {
	node, ok := tree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = NewDAGNode(dagSpec)
		tree.AddNode(node)
	}
	return node
}

// DAGNode represents a custom data type that contains a DAGSpec along with it's dependent DAGNodes
type DAGNode struct {
	DAG        models.JobSpec
	Dependents []*DAGNode
}

func (t *DAGNode) GetName() string {
	return t.DAG.Name
}

func (t *DAGNode) AddDependent(depNode *DAGNode) *DAGNode {
	t.Dependents = append(t.Dependents, depNode)
	return t
}

// NewDAGNode creates an instance of DAGNode
func NewDAGNode(dag models.JobSpec) *DAGNode {
	return &DAGNode{
		DAG:        dag,
		Dependents: []*DAGNode{},
	}
}

// MultiRootDAGTree - represents a data type which has multiple independent root nodes
// all root nodes have their independent tree based on depdencies of DAGNode.
// it also maintains a map of nodes for faster lookups and managing node data.
type MultiRootDAGTree struct {
	rootNodes []string
	dataMap   map[string]*DAGNode
}

func (t *MultiRootDAGTree) GetRootNodes() []*DAGNode {
	nodes := []*DAGNode{}
	for _, name := range t.rootNodes {
		node, _ := t.GetNodeByName(name)
		nodes = append(nodes, node)
	}
	return nodes
}

// SetRoot marks a node as root
func (t *MultiRootDAGTree) SetRoot(node *DAGNode) {
	t.rootNodes = append(t.rootNodes, node.GetName())
}

func (t *MultiRootDAGTree) AddNode(node *DAGNode) {
	t.dataMap[node.GetName()] = node
}

func (t *MultiRootDAGTree) AddNodeIfNotExist(node *DAGNode) {
	_, ok := t.GetNodeByName(node.GetName())
	if !ok {
		t.AddNode(node)
	}
}

func (t *MultiRootDAGTree) GetNodeByName(dagName string) (*DAGNode, bool) {
	value, ok := t.dataMap[dagName]
	return value, ok
}

// IsCyclic - detects if there are any cycles in the tree
func (t *MultiRootDAGTree) IsCyclic() error {
	for _, rootNode := range t.GetRootNodes() {
		err := t.hasCycle(rootNode, map[string]bool{})
		if err != nil {
			return err
		}
	}
	return nil
}

// runs a DFS on a given tree using visitor pattern
func (t *MultiRootDAGTree) hasCycle(root *DAGNode, visited map[string]bool) error {
	for _, child := range root.Dependents {
		_, ok := visited[root.GetName()]
		if !ok {
			visited[root.GetName()] = true
		} else {
			return errors.Wrap(ErrCyclicDependencyEncountered, root.GetName())
		}
		n, _ := t.GetNodeByName(child.GetName())
		return t.hasCycle(n, visited)
	}
	return nil
}

// NewMultiRootDAGTree returns an instance of multi root dag tree
func NewMultiRootDAGTree() *MultiRootDAGTree {
	return &MultiRootDAGTree{
		dataMap:   map[string]*DAGNode{},
		rootNodes: []string{},
	}
}
