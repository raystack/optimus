package v1_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/job"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"

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
	t.Run("should parse dag with status node to replay with status node", func(t *testing.T) {
		treeNode := tree.NewTreeNode(models.JobSpec{Name: "job-name"})
		nestedTreeNode := tree.NewTreeNode(models.JobSpec{Name: "nested-job-name"})
		treeNode.Dependents = append(treeNode.Dependents, nestedTreeNode)
		timeRun := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
		jobStatus := models.JobStatus{
			State:       models.InstanceStateRunning,
			ScheduledAt: timeRun,
		}
		treeNode.Runs = set.NewTreeSetWith(job.TimeOfJobStatusComparator)
		treeNode.Runs.Add(jobStatus)
		adap := v1.Adapter{}
		replayExecutionTreeNode, err := adap.ToReplayStatusTreeNode(treeNode)
		assert.Nil(t, err)
		assert.Equal(t, replayExecutionTreeNode.JobName, "job-name")
		assert.Equal(t, 1, len(replayExecutionTreeNode.Dependents))
		assert.Equal(t, replayExecutionTreeNode.Dependents[0].JobName, "nested-job-name")
		assert.Equal(t, jobStatus.State.String(), replayExecutionTreeNode.Runs[0].State)
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

		hookUnit1 := new(mock.HookPlugin)
		hookUnit1.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
			Name: "sample-hook",
		}, nil)
		defer hookUnit1.AssertExpectations(t)

		allHookRepo := new(mock.SupportedHookRepo)
		allHookRepo.On("GetByName", "sample-hook").Return(hookUnit1, nil)
		defer allHookRepo.AssertExpectations(t)

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
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobEventTypeFailure,
						Config: map[string]string{
							"key": "val",
						},
						Channels: []string{"slack://@devs"},
					},
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
			Hooks: []models.JobSpecHook{
				{
					Config: models.JobSpecConfigs{
						{
							Name:  "PROJECT",
							Value: "this",
						},
					},
					Unit: hookUnit1,
				},
			},
		}

		adapter := v1.NewAdapter(allTasksRepo, allHookRepo, nil)
		inProto, err := adapter.ToJobProto(jobSpec)
		assert.Nil(t, err)
		original, err := adapter.FromJobProto(inProto)
		assert.Equal(t, jobSpec, original)
		assert.Nil(t, err)
	})
}

func TestAdapter_FromProjectProtoWithSecrets(t *testing.T) {
	type args struct {
		conf *pb.ProjectSpecification
	}
	tests := []struct {
		name string
		args args
		want models.ProjectSpec
	}{
		{
			name: "null project should be handled correctly",
			args: args{
				conf: nil,
			},
			want: models.ProjectSpec{},
		},
		{
			name: "proto should be converted correctly",
			args: args{
				conf: &pb.ProjectSpecification{
					Name: "hello",
					Config: map[string]string{
						"key": "val",
					},
					Secrets: []*pb.ProjectSpecification_ProjectSecret{
						{
							Name:  "key",
							Value: "sec",
						},
					},
				},
			},
			want: models.ProjectSpec{
				Name: "hello",
				Config: map[string]string{
					"KEY": "val",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  "key",
						Value: "sec",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapt := &v1.Adapter{}
			if got := adapt.FromProjectProtoWithSecrets(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromProjectProtoWithSecrets() = %v, want %v", got, tt.want)
			}
		})
	}
}
