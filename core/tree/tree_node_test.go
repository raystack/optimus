package tree_test

import (
	"testing"

	"github.com/odpf/optimus/core/tree"

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
		dagNode := tree.NewTreeNode(jobSpec)
		dependentDagNode := tree.NewTreeNode(dependentJobSpec)
		dagNode.AddDependent(dependentDagNode)
		assert.Equal(t, jobName, dagNode.GetName())
	})
}
