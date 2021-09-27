package run_test

import (
	"context"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
)

func TestService(t *testing.T) {
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "bq"}, nil)
	depMod := new(mock.DependencyResolverMod)
	depMod.On("GenerateDestination", context.TODO(), mock2.AnythingOfType("models.GenerateDestinationRequest")).Return(
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
	mockedTimeNow := time.Now()
	mockedTimeFunc := func() time.Time { return mockedTimeNow }
	projSpec := models.ProjectSpec{
		Name: "proj",
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projSpec,
	}
	scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
	jobRun := models.JobRun{
		ID:          uuid.Must(uuid.NewRandom()),
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStateRunning,
		ScheduledAt: scheduledAt,
	}

	t.Run("Register", func(t *testing.T) {
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
			runRepo.On("ClearInstance", jobRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)
			runRepo.On("AddInstance", namespaceSpec, jobRun, instanceSpec).Return(nil)

			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)
			runRepo.On("GetByID", jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)
			returnedInstanceSpec, err := runService.Register(namespaceSpec, jobRun, models.InstanceTypeTask, "bq")
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
			runRepo.On("AddInstance", namespaceSpec, jobRun, instanceSpec).Return(nil)
			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)
			runRepo.On("GetByID", jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)

			returnedInstanceSpec, err := runService.Register(namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
		})
		t.Run("for hook, should not save specs if already present and return data", func(t *testing.T) {
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
			localRun := jobRun
			localRun.Instances = append(jobRun.Instances, instanceSpec)
			runRepo.On("GetByID", jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)

			returnedInstanceSpec, err := runService.Register(namespaceSpec, localRun, instanceSpec.Type, instanceSpec.Name)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
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
			runRepo.On("ClearInstance", jobRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)
			runRepo.On("AddInstance", namespaceSpec, jobRun, instanceSpec).Return(errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)

			returnedInstanceSpec, err := runService.Register(namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
	})
	t.Run("GetScheduledRun", func(t *testing.T) {
		t.Run("should update job run even if already exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", jobSpec.ID, scheduledAt).Return(jobRun, namespaceSpec, nil)
			runRepo.On("Save", namespaceSpec, models.JobRun{
				ID:          jobRun.ID,
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
			}).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)
			returnedSpec, err := runService.GetScheduledRun(namespaceSpec, jobSpec, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobRun, returnedSpec)
		})
		t.Run("should save a new job run if doesn't exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound)
			runRepo.On("Save", namespaceSpec, models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
			}).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)
			_, _ = runService.GetScheduledRun(namespaceSpec, jobSpec, scheduledAt)
		})
		t.Run("should return empty RunSpec if GetByScheduledAt returns an error", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, mockedTimeFunc, nil)
			returnedSpec, err := runService.GetScheduledRun(namespaceSpec, jobSpec, scheduledAt)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.JobRun{}, returnedSpec)
		})
	})

	t.Run("PrepInstance", func(t *testing.T) {
		t.Run("while preparing instance execution time should be correct", func(t *testing.T) {
			srv := run.NewService(nil, func() time.Time {
				return time.Now().UTC()
			}, nil)
			prep1, err := srv.PrepInstance(jobRun, models.InstanceTypeTask, "bq")
			assert.Nil(t, err)
			time.Sleep(time.Second)
			prep2, err := srv.PrepInstance(jobRun, models.InstanceTypeTask, "bq")
			assert.Nil(t, err)
			assert.NotEqual(t, prep1.Data, prep2.Data)
		})
	})
}
