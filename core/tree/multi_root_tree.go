package tree

import (
	"errors"
	"fmt"
	"sort"

	"github.com/xlab/treeprint"
)

// ErrCyclicDependencyEncountered is triggered a tree has a cyclic dependency
var ErrCyclicDependencyEncountered = errors.New("a cycle dependency encountered in the tree")

// MultiRootTree - represents a data type which has multiple independent root nodes
// all root nodes have their independent tree based on depdencies of TreeNode.
// it also maintains a map of nodes for faster lookups and managing node data.
type MultiRootTree struct {
	rootNodes []string
	dataMap   map[string]*TreeNode
}

func (t *MultiRootTree) GetRootNodes() []*TreeNode {
	nodes := []*TreeNode{}
	for _, name := range t.rootNodes {
		node, _ := t.GetNodeByName(name)
		nodes = append(nodes, node)
	}
	return nodes
}

// MarkRoot marks a node as root
func (t *MultiRootTree) MarkRoot(node *TreeNode) {
	t.rootNodes = append(t.rootNodes, node.GetName())
}

func (t *MultiRootTree) AddNode(node *TreeNode) {
	t.dataMap[node.GetName()] = node
}

func (t *MultiRootTree) AddNodeIfNotExist(node *TreeNode) {
	_, ok := t.GetNodeByName(node.GetName())
	if !ok {
		t.AddNode(node)
	}
}

func (t *MultiRootTree) GetNodeByName(dagName string) (*TreeNode, bool) {
	value, ok := t.dataMap[dagName]
	return value, ok
}

// get sorted dataMap keys
func (t *MultiRootTree) getSortedKeys() []string {
	keys := []string{}
	for k := range t.dataMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ValidateCyclic - detects if there are any cycles in the tree
func (t *MultiRootTree) ValidateCyclic() error {
	// runs a DFS on a given tree using visitor pattern
	var checkCyclic func(*TreeNode, map[string]bool, map[string]bool, *[]string) error
	checkCyclic = func(node *TreeNode, visitedNodeNames, visitedPaths map[string]bool, orderedVisitedPaths *[]string) error {
		_, isNodeVisited := visitedNodeNames[node.GetName()]
		if !isNodeVisited || !visitedNodeNames[node.GetName()] {
			visitedPaths[node.GetName()] = true
			visitedNodeNames[node.GetName()] = true
			*orderedVisitedPaths = append(*orderedVisitedPaths, node.GetName())
			var cyclicErr error
			for _, child := range node.Dependents {
				n, _ := t.GetNodeByName(child.GetName())
				_, isChildVisited := visitedNodeNames[child.GetName()]
				if !isChildVisited || !visitedNodeNames[child.GetName()] {
					cyclicErr = checkCyclic(n, visitedNodeNames, visitedPaths, orderedVisitedPaths)
				}
				if cyclicErr != nil {
					return cyclicErr
				}

				if isVisited, ok := visitedPaths[child.GetName()]; ok && isVisited {
					*orderedVisitedPaths = append(*orderedVisitedPaths, child.GetName())
					cyclicErr = fmt.Errorf("%w: %s", ErrCyclicDependencyEncountered, prettifyPaths(*orderedVisitedPaths))
				}
				if cyclicErr != nil {
					return cyclicErr
				}
			}
			visitedPaths[node.GetName()] = false
			i := 0
			for i < len(*orderedVisitedPaths) && (*orderedVisitedPaths)[i] != node.GetName() {
				i++
			}
			*orderedVisitedPaths = append((*orderedVisitedPaths)[:i], (*orderedVisitedPaths)[i+1:]...)
		}
		return nil
	}

	visitedNodeNames := make(map[string]bool)
	data := t.getSortedKeys()
	for _, k := range data {
		node := t.dataMap[k]
		if _, visited := visitedNodeNames[node.GetName()]; !visited {
			visitedPaths := map[string]bool{}
			orderedVisitedPaths := []string{}
			if err := checkCyclic(node, visitedNodeNames, visitedPaths, &orderedVisitedPaths); err != nil {
				return err
			}
		}
	}

	return nil
}

func prettifyPaths(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	i := len(paths) - 1
	root := treeprint.NewWithRoot(paths[i])
	tree := root

	for i--; i >= 0; i-- {
		tree = tree.AddBranch(paths[i])
	}

	return "\n" + root.String()
}

// NewMultiRootTree returns an instance of multi root dag tree
func NewMultiRootTree() *MultiRootTree {
	return &MultiRootTree{
		dataMap:   map[string]*TreeNode{},
		rootNodes: []string{},
	}
}
