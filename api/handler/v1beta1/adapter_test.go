package v1beta1_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/internal/lib/set"
	"github.com/odpf/optimus/internal/lib/tree"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestAdapter(t *testing.T) {
	t.Run("should parse dag node to replay node", func(t *testing.T) {
		treeNode := tree.NewTreeNode(models.JobSpec{Name: "job-name"})
		nestedTreeNode := tree.NewTreeNode(models.JobSpec{Name: "nested-job-name"})
		treeNode.Dependents = append(treeNode.Dependents, nestedTreeNode)
		timeRun := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
		treeNode.Runs.Add(timeRun)
		replayExecutionTreeNode, err := v1.ToReplayExecutionTreeNode(treeNode)
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
			State:       models.RunStateRunning,
			ScheduledAt: timeRun,
		}
		treeNode.Runs = set.NewTreeSetWith(job.TimeOfJobStatusComparator)
		treeNode.Runs.Add(jobStatus)
		replayExecutionTreeNode, err := v1.ToReplayStatusTreeNode(treeNode)
		assert.Nil(t, err)
		assert.Equal(t, "job-name", replayExecutionTreeNode.JobName)
		assert.Equal(t, 1, len(replayExecutionTreeNode.Dependents))
		assert.Equal(t, "nested-job-name", replayExecutionTreeNode.Dependents[0].JobName)
		assert.Equal(t, jobStatus.State.String(), replayExecutionTreeNode.Runs[0].State)
	})
	t.Run("should successfully parse job spec to and from proto", func(t *testing.T) {
		execUnit1 := new(mock.BasePlugin)
		execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: "sample-task",
		}, nil)
		defer execUnit1.AssertExpectations(t)

		pluginRepo := new(mock.SupportedPluginRepo)
		pluginRepo.On("GetByName", "sample-task").Return(&models.Plugin{
			Base: execUnit1,
		}, nil)

		window, err := models.NewWindow(1, "h", "1h", "48h")
		if err != nil {
			panic(err)
		}
		jobSpec := models.JobSpec{
			Version: 1,
			Name:    "test-job",
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
						On: models.JobFailureEvent,
						Config: map[string]string{
							"key": "val",
						},
						Channels: []string{"slack://@devs"},
					},
				},
			},
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit1},
				Config: models.JobSpecConfigs{
					{
						Name:  "DO",
						Value: "this",
					},
				},
				Window: window,
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
					Unit: &models.Plugin{Base: execUnit1},
				},
			},
			ExternalDependencies: models.ExternalDependency{
				HTTPDependencies: []models.HTTPDependency{
					{
						Name: "test_http_sensor_1",
						RequestParams: map[string]string{
							"key_test": "value_test",
						},
						URL: "http://test/optimus/status/1",
						Headers: map[string]string{
							"Content-Type": "application/json",
						},
					},
				},
			},
		}

		inProto := v1.ToJobSpecificationProto(jobSpec)
		original, err := v1.FromJobProto(inProto, pluginRepo)
		assert.Equal(t, jobSpec, original)
		assert.Nil(t, err)
	})
}

func TestAdapter_FromResourceProto(t *testing.T) {
	t.Run("should return empty result and error if spec is nil", func(t *testing.T) {
		var spec *pb.ResourceSpecification
		storeName := "table"
		datastoreRepo := &mock.SupportedDatastoreRepo{}

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.Empty(t, actualResource)
		assert.Error(t, actualError)
	})

	t.Run("should return empty result and error if store name is empty", func(t *testing.T) {
		spec := &pb.ResourceSpecification{}
		var storeName string
		datastoreRepo := &mock.SupportedDatastoreRepo{}

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.Empty(t, actualResource)
		assert.Error(t, actualError)
	})

	t.Run("should return empty result and error if datastore repo is nil", func(t *testing.T) {
		spec := &pb.ResourceSpecification{}
		storeName := "table"
		var datastoreRepo models.DatastoreRepo

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.Empty(t, actualResource)
		assert.Error(t, actualError)
	})

	t.Run("should return empty result and error if error encountered when getting storer", func(t *testing.T) {
		spec := &pb.ResourceSpecification{}
		storeName := "table"
		datastoreRepo := &mock.SupportedDatastoreRepo{}
		datastoreRepo.On("GetByName", tMock.Anything).Return(nil, errors.New("random error"))

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.Empty(t, actualResource)
		assert.Error(t, actualError)
	})

	t.Run("should return empty result and error if cannot find spec type from storer", func(t *testing.T) {
		spec := &pb.ResourceSpecification{}
		storeName := "table"
		types := map[models.ResourceType]models.DatastoreTypeController{}
		datastorer := &mock.Datastorer{}
		datastorer.On("Types").Return(types)
		datastoreRepo := &mock.SupportedDatastoreRepo{}
		datastoreRepo.On("GetByName", tMock.Anything).Return(datastorer, nil)

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.Empty(t, actualResource)
		assert.Error(t, actualError)
	})

	t.Run("should return spec and nil if no error is encountered", func(t *testing.T) {
		spec := &pb.ResourceSpecification{
			Type: "table",
		}
		storeName := "table"
		specAdapter := &mock.DatastoreTypeAdapter{}
		specAdapter.On("FromProtobuf", tMock.Anything).Return(models.ResourceSpec{
			Version: 1,
		}, nil)
		typeController := &mock.DatastoreTypeController{}
		typeController.On("Adapter").Return(specAdapter)
		types := map[models.ResourceType]models.DatastoreTypeController{
			"table": typeController,
		}
		datastorer := &mock.Datastorer{}
		datastorer.On("Types").Return(types)
		datastoreRepo := &mock.SupportedDatastoreRepo{}
		datastoreRepo.On("GetByName", tMock.Anything).Return(datastorer, nil)

		actualResource, actualError := v1.FromResourceProto(spec, storeName, datastoreRepo)

		assert.NotEmpty(t, actualResource)
		assert.NoError(t, actualError)
	})
}

func TestAdapter_FromInstanceProto(t *testing.T) {
	type args struct {
		conf *pb.InstanceSpec
	}
	tests := []struct {
		name string
		args args
		want models.InstanceSpec
	}{
		{
			name: "null model should be handled correctly",
			args: args{
				conf: nil,
			},
			want: models.InstanceSpec{},
		},
		{
			name: "proto should be converted correctly",
			args: args{
				conf: &pb.InstanceSpec{
					State: "running",
					Data: []*pb.InstanceSpecData{
						{
							Name:  "hello",
							Value: "world",
							Type:  pb.InstanceSpecData_TYPE_ENV,
						},
					},
					ExecutedAt: timestamppb.New(time.Date(2021, 2, 2, 2, 2, 2, 2, time.UTC)),
					Name:       "hello",
					Type:       pb.InstanceSpec_TYPE_TASK,
				},
			},
			want: models.InstanceSpec{
				Name:   "hello",
				Type:   "task",
				Status: "running",
				Data: []models.JobRunSpecData{
					{
						Name:  "hello",
						Value: "world",
						Type:  models.InstanceDataTypeEnv,
					},
				},
				ExecutedAt: time.Date(2021, 2, 2, 2, 2, 2, 2, time.UTC),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := v1.FromInstanceProto(tt.args.conf)
			assert.Nil(t, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromInstanceProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdapter_ToInstanceProto(t *testing.T) {
	type args struct {
		conf models.InstanceSpec
	}
	tests := []struct {
		name string
		args args
		want *pb.InstanceSpec
	}{
		{
			name: "proto should be converted correctly",
			args: args{
				conf: models.InstanceSpec{
					Name:   "hello",
					Type:   "task",
					Status: "running",
					Data: []models.JobRunSpecData{
						{
							Name:  "hello",
							Value: "world",
							Type:  models.InstanceDataTypeEnv,
						},
					},
					ExecutedAt: time.Date(2021, 2, 2, 2, 2, 2, 2, time.UTC),
				},
			},
			want: &pb.InstanceSpec{
				State: "running",
				Data: []*pb.InstanceSpecData{
					{
						Name:  "hello",
						Value: "world",
						Type:  pb.InstanceSpecData_TYPE_ENV,
					},
				},
				ExecutedAt: timestamppb.New(time.Date(2021, 2, 2, 2, 2, 2, 2, time.UTC)),
				Name:       "hello",
				Type:       pb.InstanceSpec_TYPE_TASK,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := v1.ToInstanceProto(tt.args.conf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToInstanceProto() = %v, want %v", got, tt.want)
			}
		})
	}
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
			if got := v1.FromProjectProtoWithSecrets(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromProjectProtoWithSecrets() = %v, want %v", got, tt.want)
			}
		})
	}
}
