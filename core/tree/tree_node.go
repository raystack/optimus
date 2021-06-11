package tree

import (
	"github.com/odpf/optimus/core/set"
)

type TreeData interface {
	GetName() string
}

// TreeNode represents a custom data type that contains data along with it's dependent TreeNodes
type TreeNode struct {
	Data       TreeData
	Dependents []*TreeNode
	Runs       set.Set
}

func (t *TreeNode) GetName() string {
	return t.Data.GetName()
}

func (t *TreeNode) AddDependent(depNode *TreeNode) *TreeNode {
	t.Dependents = append(t.Dependents, depNode)
	return t
}

// NewTreeNode creates an instance of TreeNode
func NewTreeNode(data TreeData) *TreeNode {
	return &TreeNode{
		Data:       data,
		Dependents: []*TreeNode{},
		Runs:       set.NewTreeSetWithTimeComparator(),
	}
}
