package run_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
)

func TestService(t *testing.T) {
	ctx := context.Background()
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "bq"}, nil)
	depMod := new(mock.DependencyResolverMod)
	depMod.On("GenerateDestination", ctx, mock2.AnythingOfType("models.GenerateDestinationRequest")).Return(
		&models.GenerateDestinationResponse{Destination: "proj.data.tab"}, nil)
	jobSpec := models.JobSpec{
		Name:  "foo",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: false,
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{Base: execUnit, DependencyMod: depMod},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
		Dependencies: map[string]models.JobSpecDependency{},
	}
	mockedTimeNow := time.Date(2021, 11, 21, 0, 0, 0, 0, time.UTC)
	mockedTimeFunc := func() time.Time { return mockedTimeNow }
	projSpec := models.ProjectSpec{
		Name: "proj",
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projSpec,
	}
	scheduledAt := time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	jobRun := models.JobRun{
		ID:          uuid.Must(uuid.NewRandom()),
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStateRunning,
		ScheduledAt: scheduledAt,
		ExecutedAt:  mockedTimeNow,
	}

	t.Run("Register", func(t *testing.T) {
		t.Run("for transformation, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  run.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)

			runRepo := new(mock.JobRunRepository)
			runRepo.On("AddInstance", ctx, namespaceSpec, jobRun, instanceSpec).Return(nil)
			runRepo.On("GetByID", ctx, jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, nil, nil)
			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, jobRun, models.InstanceTypeTask, "bq")
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("for transformation, should clear if present, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  run.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			runRepo := new(mock.JobRunRepository)
			runRepo.On("ClearInstance", ctx, jobRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)

			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)
			runRepo.On("GetByID", ctx, jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, nil, nil)

			var existingRun models.JobRun
			Copy(&existingRun, jobRun)
			existingRun.Spec.Task = jobRun.Spec.Task
			existingRun.Instances = append(existingRun.Instances, instanceSpec)
			existingRun.Instances[0].ExecutedAt = mockedTimeNow
			runRepo.On("AddInstance", ctx, namespaceSpec, existingRun, instanceSpec).Return(nil)

			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, existingRun, models.InstanceTypeTask, "bq")
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("for hook, should save specs if not present and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeHook,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  run.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			runRepo := new(mock.JobRunRepository)
			runRepo.On("AddInstance", ctx, namespaceSpec, jobRun, instanceSpec).Return(nil)
			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)
			runRepo.On("GetByID", ctx, jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)

			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
		})
		t.Run("for hook, should clear if its running after exceeding its schedule frequency, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeHook,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  run.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			runRepo := new(mock.JobRunRepository)

			var existingJobRun models.JobRun
			Copy(&existingJobRun, &jobRun)
			existingJobRun.Instances = append(jobRun.Instances, instanceSpec)
			existingJobRun.Spec.Task.Unit = jobRun.Spec.Task.Unit
			runRepo.On("ClearInstance", ctx, jobRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)

			var newJobRun models.JobRun
			Copy(&newJobRun, &existingJobRun)
			newJobRun.Spec.Task.Unit = jobRun.Spec.Task.Unit
			runRepo.On("AddInstance", ctx, namespaceSpec, existingJobRun, instanceSpec).Return(nil)
			runRepo.On("GetByID", ctx, jobRun.ID).Return(newJobRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)
			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, existingJobRun, models.InstanceTypeHook, "bq")
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("should return empty Instance Spec if there was any error while saving spec", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  run.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  run.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			runRepo := new(mock.JobRunRepository)
			runRepo.On("AddInstance", ctx, namespaceSpec, jobRun, instanceSpec).Return(errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)

			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
	})
	t.Run("GetScheduledRun", func(t *testing.T) {
		t.Run("should update job run even if already exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(jobRun, namespaceSpec, nil)
			runRepo.On("Save", ctx, namespaceSpec, models.JobRun{
				ID:          jobRun.ID,
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
				ExecutedAt:  mockedTimeNow,
			}).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)
			returnedSpec, err := runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobRun, returnedSpec)
		})
		t.Run("should save a new job run if doesn't exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound)
			runRepo.On("Save", ctx, namespaceSpec, models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
				ExecutedAt:  mockedTimeNow,
			}).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)
			_, _ = runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
		})
		t.Run("should return empty RunSpec if GetByScheduledAt returns an error", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil)
			returnedSpec, err := runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.JobRun{}, returnedSpec)
		})
	})
}

// Copy exported fields
func Copy(dst interface{}, src interface{}) error {
	bytes, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("failed to marshal src: %w", err)
	}
	err = json.Unmarshal(bytes, dst)
	if err != nil {
		return fmt.Errorf("failed to unmarshal into dst: %w", err)
	}
	return nil
}
