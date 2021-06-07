package v1_test

import (
	"testing"
	"time"

	v1 "github.com/odpf/optimus/api/handler/v1"
	"github.com/odpf/optimus/core/multi_root_tree"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestAdapter(t *testing.T) {
	t.Run("should parse dag node to replay node", func(t *testing.T) {
		treeNode := multi_root_tree.NewTreeNode(models.JobSpec{Name: "job-name"})
		nestedTreeNode := multi_root_tree.NewTreeNode(models.JobSpec{Name: "nested-job-name"})
		treeNode.Dependents = append(treeNode.Dependents, nestedTreeNode)
		timeRun := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
		treeNode.Runs.Add(timeRun)
		adap := v1.Adapter{}
		replayResponseNode, err := adap.ToReplayResponseNode(treeNode)
		assert.Nil(t, err)
		assert.Equal(t, replayResponseNode.JobName, "job-name")
		assert.Equal(t, 1, len(replayResponseNode.Dependents))
		assert.Equal(t, replayResponseNode.Dependents[0].JobName, "nested-job-name")
	})
}
