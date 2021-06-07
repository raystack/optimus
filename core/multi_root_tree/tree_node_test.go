package multi_root_tree_test

import (
	"testing"

	"github.com/odpf/optimus/core/multi_root_tree"

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
		dagNode := multi_root_tree.NewTreeNode(jobSpec)
		dependentDagNode := multi_root_tree.NewTreeNode(dependentJobSpec)
		dagNode.AddDependent(dependentDagNode)
		assert.Equal(t, jobName, dagNode.GetName())
	})
}
