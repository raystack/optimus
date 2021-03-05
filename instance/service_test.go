package instance_test

import (
	"testing"
	"time"

	mock2 "github.com/stretchr/testify/mock"

	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestService(t *testing.T) {
	execUnit := new(mock.ExecutionUnit)
	execUnit.On("GetName").Return("bq")
	execUnit.On("GenerateDestination", mock2.AnythingOfType("models.UnitData")).Return("proj.data.tab", nil)
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
			Unit:     execUnit,
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

	t.Run("Register", func(t *testing.T) {
		t.Run("for transformation, should clear if present, save specs and return data", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("Clear", scheduledAt).Return(nil)
			instanceSpecRepo.On("Save", instanceSpec).Return(nil)
			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, nil)
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt, models.InstanceTypeTransformation)
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("for hook, should save specs if not present and return data", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, gorm.ErrRecordNotFound).Once()
			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, nil).Once()
			instanceSpecRepo.On("Save", instanceSpec).Return(nil)
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt, models.InstanceTypeHook)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
		})
		t.Run("for hook, should not save specs if already present and return data", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, nil).Once()
			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, nil).Once()
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt, models.InstanceTypeHook)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
		})
		t.Run("should return empty Instance Spec if there was any error while saving spec", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDestination,
						Value: "proj.data.tab",
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("Clear", scheduledAt).Return(nil)
			instanceSpecRepo.On("Save", instanceSpec).Return(errors.New("a random error"))
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt, models.InstanceTypeTransformation)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
		t.Run("should return empty Instance Spec if GetByScheduledAt returns an error", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)

			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(models.InstanceSpec{}, errors.New("a random error"))
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt,
				models.InstanceTypeHook)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
	})
}
