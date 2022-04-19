package dag

import (
	"github.com/odpf/optimus/core/set"
)

type NodeNamer interface {
	GetName() string
}

// TreeNode represents a custom data type that contains data along with it's dependent TreeNodes
type TreeNode struct {
	Data  interface{}
	Edges []*TreeNode
	Runs  set.Set
}

// GetAllNodes returns level order traversal of tree starting from current node
func (t *TreeNode) GetAllNodes() []*TreeNode {
	allNodes := make([]*TreeNode, 0)
	nodesQueue := make([]*TreeNode, 0)
	nodesQueue = append(nodesQueue, t)
	for len(nodesQueue) != 0 {
		topNode := nodesQueue[0]
		nodesQueue = nodesQueue[1:]
		allNodes = append(allNodes, topNode)
		nodesQueue = append(nodesQueue, topNode.Edges...)
	}
	return allNodes
}

// GetDescendents returns all descendents using bfs
func (t *TreeNode) GetDescendents() []*TreeNode {
	allNodes := make([]*TreeNode, 0)
	seen := make(map[string]struct{})
	nodesQueue := make([]*TreeNode, 0)
	nodesQueue = append(nodesQueue, t.Edges...)
	seen[t.String()] = struct{}{}

	for len(nodesQueue) != 0 {
		topNode := nodesQueue[0]
		nodesQueue = nodesQueue[1:]

		if _, ok := seen[topNode.String()]; ok {
			continue
		}
		seen[topNode.String()] = struct{}{}

		allNodes = append(allNodes, topNode)
		nodesQueue = append(nodesQueue, topNode.Edges...)
	}
	return allNodes
}

func (t *TreeNode) String() string {
	return t.Data.(NodeNamer).GetName()
}

func (t *TreeNode) AddEdge(depNode *TreeNode) *TreeNode {
	// circular edge is not allowed
	if t.String() == depNode.String() {
		return t
	}

	t.Edges = append(t.Edges, depNode)
	return t
}

// RemoveEdge in place
func (t *TreeNode) RemoveEdge(toRemove *TreeNode) {
	idx := -1
	for i, edge := range t.Edges {
		if edge.String() == toRemove.String() {
			idx = i
		}
	}
	if idx >= 0 {
		// remove from slice, item order is not maintained
		t.Edges[idx] = t.Edges[len(t.Edges)-1]
		t.Edges = t.Edges[:len(t.Edges)-1]
	}
}

// NewTreeNode creates an instance of TreeNode with Runs
func NewTreeNode(data NodeNamer) *TreeNode {
	return &TreeNode{
		Data:  data,
		Edges: []*TreeNode{},
		Runs:  set.NewTreeSetWithTimeComparator(),
	}
}

// BasicNode creates an instance of TreeNode
func BasicNode(data NodeNamer) *TreeNode {
	return &TreeNode{
		Data:  data,
		Edges: []*TreeNode{},
	}
}
