package dag_test

import (
	"testing"

	"github.com/odpf/optimus/core/dag"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestDagNode(t *testing.T) {
	t.Run("GetNameAndDependents", func(t *testing.T) {
		jobName := "job-name"
		jobSpec := models.JobSpec{
			Name: jobName,
		}
		dependentJobName := "dependent-job-name"
		dependentJobSpec := models.JobSpec{
			Name: dependentJobName,
		}
		dagNode := dag.NewTreeNode(jobSpec)
		dependentDagNode := dag.NewTreeNode(dependentJobSpec)
		dagNode.AddEdge(dependentDagNode)
		assert.Equal(t, jobName, dagNode.String())
	})
	t.Run("GetAllNodes", func(t *testing.T) {
		treeNode := dag.TreeNode{
			Data: models.JobSpec{
				Name: "job-level-0",
			},
			Edges: []*dag.TreeNode{
				{
					Data: models.JobSpec{
						Name: "job-level-1",
					},
					Edges: []*dag.TreeNode{
						{
							Data: models.JobSpec{
								Name: "job-level-2",
							},
						},
					},
				},
			},
		}
		allNodes := treeNode.GetAllNodes()
		assert.Equal(t, 3, len(allNodes))
		assert.Equal(t, "job-level-0", allNodes[0].String())
		assert.Equal(t, "job-level-1", allNodes[1].String())
		assert.Equal(t, "job-level-2", allNodes[2].String())
	})
	t.Run("GetDescendents", func(t *testing.T) {
		treeNode := dag.TreeNode{
			Data: models.JobSpec{
				Name: "job-level-0",
			},
			Edges: []*dag.TreeNode{
				{
					Data: models.JobSpec{
						Name: "job-level-1",
					},
					Edges: []*dag.TreeNode{
						{
							Data: models.JobSpec{
								Name: "job-level-2",
							},
						},
					},
				},
			},
		}
		allNodes := treeNode.GetDescendents()
		assert.Equal(t, 2, len(allNodes))
		assert.Equal(t, "job-level-1", allNodes[0].String())
		assert.Equal(t, "job-level-2", allNodes[1].String())
	})
}
