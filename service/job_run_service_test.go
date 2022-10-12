package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

func TestJobRunService(t *testing.T) {
	ctx := context.Background()
	projSpec := models.ProjectSpec{
		Name: "proj",
	}

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
			runService := service.NewJobRunService(sch)
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
					runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
			runService := service.NewJobRunService(sch)
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
	window, err := models.NewWindow(1, "d", "0", "1h")
	if err != nil {
		panic(err)
	}
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
			Window:   window,
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
