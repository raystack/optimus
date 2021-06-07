package multi_root_tree

import (
	"github.com/pkg/errors"
)

var (
	// ErrCyclicDependencyEncountered is triggered a tree has a cyclic dependency
	ErrCyclicDependencyEncountered = errors.New("a cycle dependency encountered in the tree")
)

// MultiRootDAGTree - represents a data type which has multiple independent root nodes
// all root nodes have their independent tree based on depdencies of TreeNode.
// it also maintains a map of nodes for faster lookups and managing node data.
type MultiRootDAGTree struct {
	rootNodes []string
	dataMap   map[string]*TreeNode
}

func (t *MultiRootDAGTree) GetRootNodes() []*TreeNode {
	nodes := []*TreeNode{}
	for _, name := range t.rootNodes {
		node, _ := t.GetNodeByName(name)
		nodes = append(nodes, node)
	}
	return nodes
}

// MarkRoot marks a node as root
func (t *MultiRootDAGTree) MarkRoot(node *TreeNode) {
	t.rootNodes = append(t.rootNodes, node.GetName())
}

func (t *MultiRootDAGTree) AddNode(node *TreeNode) {
	t.dataMap[node.GetName()] = node
}

func (t *MultiRootDAGTree) AddNodeIfNotExist(node *TreeNode) {
	_, ok := t.GetNodeByName(node.GetName())
	if !ok {
		t.AddNode(node)
	}
}

func (t *MultiRootDAGTree) GetNodeByName(dagName string) (*TreeNode, bool) {
	value, ok := t.dataMap[dagName]
	return value, ok
}

// IsCyclic - detects if there are any cycles in the tree
func (t *MultiRootDAGTree) IsCyclic() error {
	visitedMap := make(map[string]bool)
	for _, node := range t.dataMap {
		if _, visited := visitedMap[node.GetName()]; !visited {
			pathMap := make(map[string]bool)
			err := t.hasCycle(node, visitedMap, pathMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// runs a DFS on a given tree using visitor pattern
func (t *MultiRootDAGTree) hasCycle(root *TreeNode, visited, pathMap map[string]bool) error {
	_, isNodeVisited := visited[root.GetName()]
	if !isNodeVisited || !visited[root.GetName()] {
		pathMap[root.GetName()] = true
		visited[root.GetName()] = true
		var cyclicErr error
		for _, child := range root.Dependents {
			n, _ := t.GetNodeByName(child.GetName())
			_, isChildVisited := visited[child.GetName()]
			if !isChildVisited || !visited[child.GetName()] {
				cyclicErr = t.hasCycle(n, visited, pathMap)
			}
			if cyclicErr != nil {
				return cyclicErr
			}

			_, childAlreadyInPath := pathMap[child.GetName()] // 1 -> 2 -> 1
			if childAlreadyInPath && pathMap[child.GetName()] {
				cyclicErr = errors.Wrap(ErrCyclicDependencyEncountered, root.GetName())
			}
			if cyclicErr != nil {
				return cyclicErr
			}
		}
		pathMap[root.GetName()] = false
	}
	return nil
}

// NewMultiRootDAGTree returns an instance of multi root dag tree
func NewMultiRootDAGTree() *MultiRootDAGTree {
	return &MultiRootDAGTree{
		dataMap:   map[string]*TreeNode{},
		rootNodes: []string{},
	}
}
