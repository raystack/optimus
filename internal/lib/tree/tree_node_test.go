package tree_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/lib/tree"
)

type testNode struct {
	Name string
}

func (t testNode) GetName() string {
	return t.Name
}

func TestDagNode(t *testing.T) {
	t.Run("GetNameAndDependents", func(t *testing.T) {
		jobName := "job-name"
		jobNode := testNode{
			Name: jobName,
		}
		dependentJobName := "dependent-job-name"
		dependentJobSpec := testNode{
			Name: dependentJobName,
		}
		dagNode := tree.NewTreeNode(jobNode)
		dependentDagNode := tree.NewTreeNode(dependentJobSpec)
		dagNode.AddDependent(dependentDagNode)
		assert.Equal(t, jobName, dagNode.GetName())
	})
	t.Run("GetAllNodes", func(t *testing.T) {
		treeNode := tree.TreeNode{
			Data: testNode{
				Name: "job-level-0",
			},
			Dependents: []*tree.TreeNode{
				{
					Data: testNode{
						Name: "job-level-1",
					},
					Dependents: []*tree.TreeNode{
						{
							Data: testNode{
								Name: "job-level-2",
							},
						},
					},
				},
			},
		}
		allNodes := treeNode.GetAllNodes()
		assert.Equal(t, 3, len(allNodes))
		assert.Equal(t, "job-level-0", allNodes[0].Data.GetName())
		assert.Equal(t, "job-level-1", allNodes[1].Data.GetName())
		assert.Equal(t, "job-level-2", allNodes[2].Data.GetName())
	})
}
