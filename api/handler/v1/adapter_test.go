package v1_test

import (
	"context"
	"testing"
	"time"

	"github.com/odpf/optimus/mock"

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
	t.Run("should successfully parse job spec to and from proto", func(t *testing.T) {
		execUnit1 := new(mock.TaskPlugin)
		execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
			Name: "sample-task",
		}, nil)
		defer execUnit1.AssertExpectations(t)

		allTasksRepo := new(mock.SupportedTaskRepo)
		allTasksRepo.On("GetByName", "sample-task").Return(execUnit1, nil)
		defer allTasksRepo.AssertExpectations(t)

		jobSpec := models.JobSpec{
			Name: "test-job",
			Schedule: models.JobSpecSchedule{
				StartDate: time.Date(2021, 10, 6, 0, 0, 0, 0, time.UTC),
				Interval:  "@daily",
			},
			Behavior: models.JobSpecBehavior{
				DependsOnPast: false,
				CatchUp:       true,
				Retry: models.JobSpecBehaviorRetry{
					Count:              5,
					Delay:              0,
					ExponentialBackoff: true,
				},
			},
			Task: models.JobSpecTask{
				Unit: execUnit1,
				Config: models.JobSpecConfigs{
					{
						Name:  "DO",
						Value: "this",
					},
				},
				Window: models.JobSpecTaskWindow{
					Size:       time.Hour * 48,
					Offset:     time.Hour,
					TruncateTo: "h",
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				},
			),
			Dependencies: map[string]models.JobSpecDependency{},
		}

		adapter := v1.NewAdapter(allTasksRepo, nil, nil)
		inProto, err := adapter.ToJobProto(jobSpec)
		assert.Nil(t, err)
		original, err := adapter.FromJobProto(inProto)
		assert.Equal(t, jobSpec, original)
		assert.Nil(t, err)
	})
}
