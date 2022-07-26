package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

func TestJobRunService(t *testing.T) {
	ctx := context.Background()
	projSpec := models.ProjectSpec{
		Name: "proj",
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projSpec,
	}
	jobSpec := getJobSpec(namespaceSpec)
	mockedTimeNow := time.Date(2021, 11, 21, 0, 0, 0, 0, time.UTC)
	mockedTimeFunc := func() time.Time { return mockedTimeNow }
	scheduledAt := time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	jobRun := models.JobRun{
		ID:          uuid.Must(uuid.NewRandom()),
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStateRunning,
		ScheduledAt: scheduledAt,
		ExecutedAt:  mockedTimeNow,
	}
	jobDestination := "project.dataset.table"
	pluginService := new(mock.DependencyResolverPluginService)
	pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(
		&models.GenerateDestinationResponse{Destination: jobDestination}, nil)

	startTime, err := jobSpec.Task.Window.GetStartTime(mockedTimeNow)
	if err != nil {
		panic(err)
	}
	endTime, err := jobSpec.Task.Window.GetEndTime(mockedTimeNow)
	if err != nil {
		panic(err)
	}

	t.Run("Register", func(t *testing.T) {
		t.Run("for transformation, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.JobRunSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDestination,
						Value: jobDestination,
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			localRun := getCopy(jobRun)
			localRun.Instances = append(localRun.Instances, instanceSpec)

			runRepo := new(mock.JobRunRepository)
			runRepo.On("ClearInstance", ctx, localRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)
			runRepo.On("AddInstance", ctx, namespaceSpec, localRun, instanceSpec).Return(nil)
			runRepo.On("GetByID", ctx, localRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, nil, nil, pluginService)
			returnedInstanceSpec, err := jobRunService.Register(ctx, namespaceSpec, localRun, models.InstanceTypeTask, "bq")

			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})

		t.Run("for transformation, should clear if present, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.JobRunSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDestination,
						Value: jobDestination,
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			localRun := getCopy(jobRun)
			localRun.Instances = append(localRun.Instances, instanceSpec)

			runRepo := new(mock.JobRunRepository)
			runRepo.On("ClearInstance", ctx, localRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)
			runRepo.On("GetByID", ctx, localRun.ID).Return(localRun, namespaceSpec, nil)
			runRepo.On("AddInstance", ctx, namespaceSpec, localRun, instanceSpec).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, nil, nil, pluginService)

			returnedInstanceSpec, err := jobRunService.Register(ctx, namespaceSpec, localRun, models.InstanceTypeTask, "bq")
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("for hook, should save specs if not present and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeHook,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.JobRunSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDestination,
						Value: jobDestination,
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			localRun := getCopy(jobRun)
			localRun.Instances = append(localRun.Instances, instanceSpec)
			runRepo := new(mock.JobRunRepository)
			runRepo.On("ClearInstance", ctx, localRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)
			runRepo.On("AddInstance", ctx, namespaceSpec, localRun, instanceSpec).Return(nil)
			runRepo.On("GetByID", ctx, localRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)

			returnedInstanceSpec, err := jobRunService.Register(ctx, namespaceSpec, localRun, instanceSpec.Type, instanceSpec.Name)
			assert.Nil(t, err)
			assert.Equal(t, returnedInstanceSpec, instanceSpec)
		})
		t.Run("for hook, should clear if its running after exceeding its schedule frequency, save specs and return data", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeHook,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.JobRunSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDestination,
						Value: jobDestination,
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			runRepo := new(mock.JobRunRepository)

			localRun := getCopy(jobRun)
			localRun.Instances = append(localRun.Instances, instanceSpec)
			runRepo.On("ClearInstance", ctx, localRun.ID, instanceSpec.Type, instanceSpec.Name).Return(nil)

			runRepo.On("AddInstance", ctx, namespaceSpec, localRun, instanceSpec).Return(nil)
			runRepo.On("GetByID", ctx, jobRun.ID).Return(localRun, namespaceSpec, nil)
			defer runRepo.AssertExpectations(t)

			runService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)
			returnedInstanceSpec, err := runService.Register(ctx, namespaceSpec, localRun, models.InstanceTypeHook, "bq")
			assert.Nil(t, err)
			assert.Equal(t, instanceSpec, returnedInstanceSpec)
		})
		t.Run("should return empty Instance Spec if there was any error while saving spec", func(t *testing.T) {
			instanceSpec := models.InstanceSpec{
				Name:       "bq",
				Type:       models.InstanceTypeTask,
				ExecutedAt: mockedTimeNow,
				Status:     models.RunStateRunning,
				Data: []models.JobRunSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDestination,
						Value: jobDestination,
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			localRun := getCopy(jobRun)
			runRepo := new(mock.JobRunRepository)
			runRepo.On("AddInstance", ctx, namespaceSpec, localRun, instanceSpec).Return(errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)

			returnedInstanceSpec, err := jobRunService.Register(ctx, namespaceSpec, localRun, instanceSpec.Type, instanceSpec.Name)
			assert.Equal(t, "a random error", err.Error())
			assert.Equal(t, models.InstanceSpec{}, returnedInstanceSpec)
		})
	})

	t.Run("GetScheduledRun", func(t *testing.T) {
		t.Run("should not fail if dependency module is not found in plugin service", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(jobRun, namespaceSpec, nil)
			jobDestination := "://"
			runRepo.On("Save", ctx, namespaceSpec, models.JobRun{
				ID:          jobRun.ID,
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
				ExecutedAt:  mockedTimeNow,
			}, jobDestination).Return(nil)
			defer runRepo.AssertExpectations(t)

			noDepModpluginService := new(mock.DependencyResolverPluginService)
			noDepModpluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(
				&models.GenerateDestinationResponse{}, service.ErrDependencyModNotFound)

			runService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, noDepModpluginService)
			returnedSpec, err := runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobRun, returnedSpec)
		})
		t.Run("should update job run even if already exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(jobRun, namespaceSpec, nil)
			jobDestination := "://project.dataset.table"
			runRepo.On("Save", ctx, namespaceSpec, models.JobRun{
				ID:          jobRun.ID,
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
				ExecutedAt:  mockedTimeNow,
			}, jobDestination).Return(nil)
			defer runRepo.AssertExpectations(t)

			runService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)
			returnedSpec, err := runService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobRun, returnedSpec)
		})
		t.Run("should save a new job run if doesn't exists", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound)
			jobDestination := "://project.dataset.table"
			runRepo.On("Save", ctx, namespaceSpec, models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStatePending,
				ScheduledAt: scheduledAt,
				ExecutedAt:  mockedTimeNow,
			}, jobDestination).Return(nil)
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)
			_, _ = jobRunService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
		})
		t.Run("should return empty RunSpec if GetByScheduledAt returns an error", func(t *testing.T) {
			runRepo := new(mock.JobRunRepository)
			runRepo.On("GetByScheduledAt", ctx, jobSpec.ID, scheduledAt).Return(models.JobRun{}, models.NamespaceSpec{}, errors.New("a random error"))
			defer runRepo.AssertExpectations(t)

			jobRunService := service.NewJobRunService(runRepo, mockedTimeFunc, nil, pluginService)
			returnedSpec, err := jobRunService.GetScheduledRun(ctx, namespaceSpec, jobSpec, scheduledAt)
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
		jobCron, err := cron.ParseCronSchedule("0 12 * * *")
		if err != nil {
			t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
		}
		t.Run("should not able to get job runs when scheduler returns empty response", func(t *testing.T) {
			sch := new(mock.Scheduler)
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Version: 1,
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch.On("GetJobRuns", ctx, spec, jobQuery, jobCron).Return([]models.JobRun{}, nil)
			defer sch.AssertExpectations(t)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.Nil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should able to get job runs when scheduler returns valid response", func(t *testing.T) {
			spec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpec := models.JobSpec{
				Version: 1,
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			runsFromScheduler, err := mockGetJobRuns(5, startDate, jobSpec.Schedule.Interval, models.RunStateSuccess.String())
			if err != nil {
				t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
			}
			runsFromSchFor3days, err := mockGetJobRuns(3, startDate, jobSpec.Schedule.Interval, models.RunStateSuccess.String())
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for success state %v", err)
			}
			expPendingRuns, err := mockGetJobRuns(2, startDate.Add(time.Hour*24*3), jobSpec.Schedule.Interval, models.RunStatePending.String())
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
					description: "no filterRuns applied",
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
					description: "when some job instances are not started by scheduler and no filterRuns applied",
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
					sch.On("GetJobRuns", ctx, spec, scenario.input, jobCron).Return(scenario.runs, nil)
					defer sch.AssertExpectations(t)
					runService := service.NewJobRunService(nil, nil, sch, pluginService)
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
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate.Add(-time.Hour * 24 * 2),
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when invalid cron interval present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "invalid interval",
				},
			}
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when no cron interval present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "",
				},
			}
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when no start date present at DB", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Schedule: models.JobSpecSchedule{
					EndDate:  nil,
					Interval: "0 12 * * *",
				},
			}
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch := new(mock.Scheduler)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
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
			jobQuery := &models.JobQuery{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			sch.On("GetJobRuns", ctx, spec, jobQuery, jobCron).Return([]models.JobRun{}, errors.New("failed: due to invalid URL"))
			defer sch.AssertExpectations(t)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.NotNil(t, err, errors.New("failed: due to invalid URL"))
			assert.Nil(t, nil, returnedRuns)
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

			jobQuery := &models.JobQuery{
				Name:        "sample_select",
				OnlyLastRun: true,
			}
			runs := []models.JobRun{
				{
					Status:      models.JobRunState("success"),
					ScheduledAt: endDate,
				},
			}
			sch.On("GetJobRuns", ctx, spec, jobQuery, jobCron).Return(runs, nil)
			defer sch.AssertExpectations(t)
			runService := service.NewJobRunService(nil, nil, sch, pluginService)
			returnedRuns, err := runService.GetJobRunList(ctx, projSpec, jobSpec, jobQuery)
			assert.Nil(t, err)
			assert.Equal(t, runs, returnedRuns)
		})
	})
}

func getCopy(original models.JobRun) models.JobRun {
	return models.JobRun{
		ID:      original.ID,
		Spec:    original.Spec,
		Trigger: original.Trigger,
		Status:  original.Status,
		// skip Instances as they are not in the original top level
		ScheduledAt: original.ExecutedAt,
		ExecutedAt:  original.ExecutedAt,
	}
}

func getJobSpec(namespaceSpec models.NamespaceSpec) models.JobSpec {
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "bq"}, nil)
	return models.JobSpec{
		Version: 1,
		Name:    "foo",
		Owner:   "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: false,
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{Base: execUnit},
			Priority: 2000,
			Window: models.WindowV1{
				Size:       "1h",
				Offset:     "0",
				TruncateTo: "d",
			},
		},
		Dependencies:  map[string]models.JobSpecDependency{},
		NamespaceSpec: namespaceSpec,
	}
}

func mockGetJobRuns(afterDays int, date time.Time, interval, status string) ([]models.JobRun, error) {
	var expRuns []models.JobRun
	schSpec, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return expRuns, err
	}
	nextStart := schSpec.Next(date.Add(-time.Second * 1))
	for i := 0; i < afterDays; i++ {
		expRuns = append(expRuns, models.JobRun{
			Status:      models.JobRunState(status),
			ScheduledAt: nextStart,
		})
		nextStart = schSpec.Next(nextStart)
	}
	return expRuns, nil
}
