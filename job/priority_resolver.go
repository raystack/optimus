package job

import (
	"errors"
	"fmt"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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

	// ErrJobSpecNotFound is thrown when a DAG was not found while looking up
	ErrJobSpecNotFound = "job spec not found %s"

	// ErrCyclicDependencyEncountered is triggered a tree has a cyclic dependency
	ErrCyclicDependencyEncountered = "a cycle dependency encountered in the tree"
)

// PriorityResolver defines an interface that represents getting
// priority weight of DAGs based on certain algorithm
type PriorityResolver interface {
	GetByDAG(models.JobSpec) (int, error)
}

// priorityResolver runs a breadth first traversal on DAG dependencies trees
// and returns highest weight for the DAG that do not have any dependencies, dynamically.
// eg, consider following DAGs and dependencies: [dag1 <- dag2 <- dag3] [dag4] [dag5 <- dag6]
// In this example, we've 6 DAGs in which dag1, dag2, dag5 have dependent DAGs. which means,
// It'd be preferable to run dag1, dag4, dag5 before other DAGs. Results would be:
// dag1, dag4, dag5 will get highest weight (maxWeight)
// dag2, dag6 will get weight of maxWeight-1
// dag3 will get maxWeight-2
type priorityResolver struct {
	dagSpecRepo    store.JobSpecRepository
	dagResolver    DependencyResolver
	dagWeightCache map[string]int
	assigned       bool
}

// GetByDAG - returns priority weight of a DAG. evaluates and caches weights if not assigned already.
func (a *priorityResolver) GetByDAG(dagSpec models.JobSpec) (int, error) {
	if !a.assigned {
		if err := a.evaluateWeightsMap(); err != nil {
			return MinPriorityWeight, err
		}
	}
	weight, ok := a.dagWeightCache[dagSpec.Name]
	if !ok {
		return MinPriorityWeight, nil
	}
	return weight, nil
}

// evaluateWeightsMap - converts the DAGSpecRepo into the MultiRootTree and runs
// a BFS on each tree to determine the priority weights
func (a *priorityResolver) evaluateWeightsMap() error {
	multiRootTree, err := ConvertDAGSpecRepoToMultiRootTree(a.dagSpecRepo, a.dagResolver)
	if err != nil {
		return err
	}

	// perform a breadth first traversal on all trees and assign weights.
	// higher weights are assign to the nodes on the top, and the weight
	// reduces as we go down the tree level
	a.dagWeightCache = make(map[string]int)
	a.assignWeight(multiRootTree.GetRootNodes(), MaxPriorityWeight)
	a.assigned = true
	return nil
}

func (a *priorityResolver) assignWeight(rootNodes []*DAGNode, weight int) {
	subChildren := []*DAGNode{}
	for _, rootNode := range rootNodes {
		a.dagWeightCache[rootNode.GetName()] = weight
		subChildren = append(subChildren, rootNode.Dependents...)
	}
	if len(subChildren) > 0 {
		a.assignWeight(subChildren, weight-PriorityWeightGap)
	}
}

// NewPriorityResolver create an instance of priorityResolver
// it accepts the dagSpecRepo and dagSpecs as mandatory parameters
func NewPriorityResolver(dagSpecRepo store.JobSpecRepository, dagResolver DependencyResolver) *priorityResolver {
	return &priorityResolver{
		dagSpecRepo: dagSpecRepo,
		dagResolver: dagResolver,
		assigned:    false,
	}
}

// ConvertDAGSpecRepoToMultiRootTree - converts the DAGSpecRepo into the MultiRootTree
// by resolving and looking into the dependencies of each DAG.
func ConvertDAGSpecRepoToMultiRootTree(dagSpecRepo store.JobSpecRepository, dagResolver DependencyResolver) (*MultiRootDAGTree, error) {
	// creates a map with DAGName and DAGSpec with resolved deps
	// Note: it's crucial to have resolved dependencies beforehand.
	// getDAGSpecMap accepts a DAGResolver that resolves the dependencies
	dagSpecMap, err := getDAGSpecMapWithDependencies(dagSpecRepo, dagResolver)
	if err != nil {
		return nil, err
	}

	// build a multi root tree and assign dependencies
	tree := NewMultiRootDAGTree()
	for _, childSpec := range dagSpecMap {
		childNode := findOrCreateDAGNode(tree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			parentSpec, ok := dagSpecMap[depDAG.Job.Name]
			if !ok {
				return nil, fmt.Errorf(ErrJobSpecNotFound, depDAG.Job.Name)
			}
			parentNode := findOrCreateDAGNode(tree, parentSpec)
			parentNode.AddDependent(childNode)
			tree.AddNode(parentNode)
		}

		// the DAGs with no dependencies are root nodes for the tree
		if len(childSpec.Dependencies) == 0 {
			tree.SetRoot(childNode)
		}
	}

	if err = tree.IsCyclic(); err != nil {
		return nil, err
	}
	return tree, nil
}

// getDAGSpecMapWithDependencies - returns map[string]models.Job after resolving dependencies of each DAGSpec
func getDAGSpecMapWithDependencies(dagSpecRepo store.JobSpecRepository, dagResolver DependencyResolver) (map[string]models.JobSpec, error) {
	specs, err := dagSpecRepo.GetAll()
	if err != nil {
		return nil, err
	}

	specs, err = dagResolver.Resolve(specs)
	if err != nil {
		return nil, err
	}

	dagSpecMap := make(map[string]models.JobSpec)
	for _, dagSpec := range specs {
		dagSpecMap[dagSpec.Name] = dagSpec
	}

	return dagSpecMap, nil
}

func findOrCreateDAGNode(tree *MultiRootDAGTree, dagSpec models.JobSpec) *DAGNode {
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

func (t DAGNode) String() string {
	depNames := ""
	for _, n := range t.Dependents {
		depNames += n.GetName() + ", "
	}
	return fmt.Sprintf("DAGNode(%s) << [%s]", t.GetName(), depNames)
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
			return errors.New(ErrCyclicDependencyEncountered)
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
