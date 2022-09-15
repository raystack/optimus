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

// IsCyclic - detects if there are any cycles in the tree
func (t *MultiRootTree) IsCyclic() error {
	visitedMap := make(map[string]bool)
	data := t.getSortedKeys()
	for _, k := range data {
		node := t.dataMap[k]
		if _, visited := visitedMap[node.GetName()]; !visited {
			err := t.isCyclic(node, visitedMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (t *MultiRootTree) isCyclic(root *TreeNode, visited map[string]bool) error {
	pathMap := make(map[string]bool)
	paths := []string{}
	return t.hasCycle(root, visited, pathMap, paths)
}

// runs a DFS on a given tree using visitor pattern
func (t *MultiRootTree) hasCycle(root *TreeNode, visited, pathMap map[string]bool, paths []string) error {
	_, isNodeVisited := visited[root.GetName()]
	if !isNodeVisited || !visited[root.GetName()] {
		pathMap[root.GetName()] = true
		visited[root.GetName()] = true
		paths = append(paths, root.GetName())
		var cyclicErr error
		for _, child := range root.Dependents {
			n, _ := t.GetNodeByName(child.GetName())
			_, isChildVisited := visited[child.GetName()]
			if !isChildVisited || !visited[child.GetName()] {
				cyclicErr = t.hasCycle(n, visited, pathMap, paths)
			}
			if cyclicErr != nil {
				return cyclicErr
			}

			_, childAlreadyInPath := pathMap[child.GetName()] // 1 -> 2 -> 1
			if childAlreadyInPath && pathMap[child.GetName()] {
				paths = append(paths, child.GetName())
				cyclicErr = fmt.Errorf("%w: %s", ErrCyclicDependencyEncountered, stringifyPaths(paths))
			}
			if cyclicErr != nil {
				return cyclicErr
			}
		}
		pathMap[root.GetName()] = false
		i := 0
		for i < len(paths) && paths[i] != root.GetName() {
			i++
		}
		paths = append(paths[:i], paths[i+1:]...)
	}
	return nil
}

func stringifyPaths(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	root := treeprint.NewWithRoot(paths[len(paths)-1])
	tree := root

	for i := len(paths) - 2; i >= 0; i-- {
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
