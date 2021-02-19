package instance_test

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"
	"testing"
	"time"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestService(t *testing.T) {
	execUnit := new(mock.ExecutionUnit)
	execUnit.On("GetName").Return("bq")
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
		t.Run("should save specs and return with data", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  "EXECUTION_TIME",
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DSTART",
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DEND",
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("Save", instanceSpec).Return(nil)
			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(instanceSpec, nil)
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt)
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
						Name:  "EXECUTION_TIME",
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DSTART",
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DEND",
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("Save", instanceSpec).Return(errors.New("a random error"))
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})

		t.Run("should return empty Instance Spec if GetByScheduledAt returns an error", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  "EXECUTION_TIME",
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DSTART",
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DEND",
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			instanceSpecRepo.On("Save", instanceSpec).Return(nil)
			instanceSpecRepo.On("GetByScheduledAt", scheduledAt).Return(models.InstanceSpec{}, errors.New("a random error"))
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			returnedInstanceSpec, err := instanceService.Register(jobSpec, scheduledAt)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
	})

	t.Run("Clear", func(t *testing.T) {
		t.Run("should clear the instanceSpec data from specs", func(t *testing.T) {
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpecRepo := new(mock.InstanceSpecRepository)
			instanceSpecRepo.On("Clear", scheduledAt).Return(nil)
			defer instanceSpecRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.InstanceSpecRepoFactory)
			jobRunSpecRep.On("New", jobSpec).Return(instanceSpecRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			instanceService := instance.NewService(jobRunSpecRep, mockedTimeFunc)

			err := instanceService.Clear(jobSpec, scheduledAt)
			assert.Nil(t, err)
		})
	})
}
