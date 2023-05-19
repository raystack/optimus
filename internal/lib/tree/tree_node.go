package tree

type TreeData interface {
	GetName() string
}

// TreeNode represents a custom data type that contains data along with it's dependent TreeNodes
type TreeNode struct {
	Data       TreeData
	Dependents []*TreeNode
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
		nodesQueue = append(nodesQueue, topNode.Dependents...)
	}
	return allNodes
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
	}
}
