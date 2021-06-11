package v1_test

import (
	"testing"
	"time"

	v1 "github.com/odpf/optimus/api/handler/v1"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestAdapter(t *testing.T) {
	t.Run("should parse dag node to replay node", func(t *testing.T) {
		treeNode := tree.NewTreeNode(models.JobSpec{Name: "job-name"})
		nestedTreeNode := tree.NewTreeNode(models.JobSpec{Name: "nested-job-name"})
		treeNode.Dependents = append(treeNode.Dependents, nestedTreeNode)
		timeRun := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
		treeNode.Runs.Add(timeRun)
		adap := v1.Adapter{}
		replayExecutionTreeNode, err := adap.ToReplayExecutionTreeNode(treeNode)
		assert.Nil(t, err)
		assert.Equal(t, replayExecutionTreeNode.JobName, "job-name")
		assert.Equal(t, 1, len(replayExecutionTreeNode.Dependents))
		assert.Equal(t, replayExecutionTreeNode.Dependents[0].JobName, "nested-job-name")
	})
}
