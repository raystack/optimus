package run_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/odpf/optimus/core/cron"
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

			runService := run.NewService(jobRunSpecRep, nil, nil, nil, nil)
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

			runService := run.NewService(jobRunSpecRep, nil, nil, nil, nil)

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

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)

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

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)
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

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)

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

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)
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

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)
			_, _ = runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
		})
		t.Run("should return empty RunSpec if GetByScheduledAt returns an error", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunSpecRep := new(mock.JobRunRepoFactory)
			jobRunSpecRep.On("New").Return(runRepo, nil)
			defer jobRunSpecRep.AssertExpectations(t)

			runService := run.NewService(jobRunSpecRep, nil, mockedTimeFunc, nil, nil)
			returnedSpec, err := runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.JobRun{}, returnedSpec)
		})
	})

	t.Run("GetJobRunList", func(t *testing.T) {
		startDate, err := time.Parse(time.RFC3339, "2022-03-20T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		endDate, err := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		t.Run("should not able to get job runs when scheduler returns empty response", func(t *testing.T) {
			sch := new(mock.Scheduler)
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch.On("GetJobRuns", ctx, spec, param).Return([]models.JobRun{}, nil)
			defer sch.AssertExpectations(t)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.Nil(t, err)
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should able to get job runs when scheduler returns valid response", func(t *testing.T) {
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			runsFromScheduler, err := buildMockGetJobRuns(6, startDate, jobSpec.Schedule.Interval, models.RunStateSuccess.String())
			if err != nil {
				t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
			}
			runsFromSchFor3days, err := buildMockGetJobRuns(3, startDate, jobSpec.Schedule.Interval, models.RunStateSuccess.String())
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for success state %v", err)
			}
			expPendingRuns, err := buildMockGetJobRuns(3, startDate.Add(time.Hour*24*3), jobSpec.Schedule.Interval, models.RunStatePending.String())
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for pending state %v", err)
			}
			type cases struct {
				description    string
				input          *models.JobQuery
				runs           []models.JobRun
				job            models.JobSpec
				expectedResult []models.JobRun
			}
			for _, scenario := range []cases{
				{
					description: "filtering based on success",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{models.RunStateSuccess.String()},
					},
					job:            jobSpec,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on failed",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{models.RunStateFailed.String()},
					},
					job:            jobSpec,
					expectedResult: nil,
				},
				{
					description: "no filter applied",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobSpec,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on pending",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{models.RunStatePending.String()},
					},
					job:            jobSpec,
					runs:           runsFromScheduler,
					expectedResult: nil,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on pending status",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{models.RunStatePending.String()},
					},
					job:            jobSpec,
					runs:           runsFromSchFor3days,
					expectedResult: expPendingRuns,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on success status",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{models.RunStateSuccess.String()},
					},
					job:            jobSpec,
					runs:           runsFromSchFor3days,
					expectedResult: runsFromSchFor3days,
				},
				{
					description: "when some job instances are not started by scheduler and no filter applied",
					input: &models.JobQuery{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobSpec,
					runs:           runsFromSchFor3days,
					expectedResult: append(runsFromSchFor3days, expPendingRuns...),
				},
			} {
				t.Run(scenario.description, func(t *testing.T) {
					sch := new(mock.Scheduler)
					sch.On("GetJobRuns", ctx, spec, scenario.input).Return(scenario.runs, nil)
					defer sch.AssertExpectations(t)
					runService := run.NewService(nil, nil, nil, sch, nil)
					returnedRuns, err := runService.GetJobRunList(ctx, projSpec, scenario.job, scenario.input)
					assert.Nil(t, err)
					assert.Equal(t, scenario.expectedResult, returnedRuns)
				})
			}
		})
		t.Run("should not able to get job runs when invalid date range is given", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate.Add(-time.Hour * 24 * 2),
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should not able to get job runs when invalid cron interval present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "invalid interval",
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should not able to get job runs when no start date present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					EndDate:  nil,
					Interval: "0 12 * * *",
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should not able to get job runs when no cron interval is present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should not able to get job runs when scheduler returns an error", func(t *testing.T) {
			sch := new(mock.Scheduler)
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			param := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch.On("GetJobRuns", ctx, spec, param).Return([]models.JobRun{}, errors.New("failed: due to invalid URL"))
			defer sch.AssertExpectations(t)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.NotNil(t, err, errors.New("failed: due to invalid URL"))
			assert.Nil(t, nil, returnedSpec)
		})
		t.Run("should able to get job runs when only last run is required", func(t *testing.T) {
			sch := new(mock.Scheduler)
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			param := &models.JobQuery{
				Name:        "sample_select",
				OnlyLastRun: true,
			}
			runs := []models.JobRun{
				{
					Status:      models.JobRunState("success"),
					ScheduledAt: endDate,
				},
			}

			sch.On("GetJobRuns", ctx, spec, param).Return(runs, nil)
			defer sch.AssertExpectations(t)
			runService := run.NewService(nil, nil, nil, sch, nil)
			returnedSpec, err := runService.GetJobRunList(ctx, projSpec, jobSpec, param)
			assert.Nil(t, err)
			assert.Equal(t, runs, returnedSpec)
		})
	})
}

// Copy exported fields
func Copy(dst interface{}, src interface{}) error {
	bytes, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("failed to marshal src: %v", err)
	}
	err = json.Unmarshal(bytes, dst)
	if err != nil {
		return fmt.Errorf("failed to unmarshal into dst: %s", err)
	}
	return nil
}

func buildMockGetJobRuns(afterDays int, date time.Time, interval string, status string) ([]models.JobRun, error) {
	var expRuns []models.JobRun
	schSpec, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return expRuns, err
	}
	nextStart := schSpec.Next(date)
	dur := schSpec.Interval(date)
	for i := 0; i < afterDays; i++ {
		expRuns = append(expRuns, models.JobRun{
			Status:      models.JobRunState(status),
			ScheduledAt: nextStart.Add(-dur),
		})
		nextStart = nextStart.Add(dur)
	}
	return expRuns, nil
}
