package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
)

//TODO: add tests for missing pieces
type JobRepository struct {
	mock.Mock
}

func (j *JobRepository) GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	args := j.Called(ctx, name, jobName)
	return args.Get(0).(*scheduler.Job), args.Error(1)
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName, jobName)
	return args.Get(0).(*scheduler.JobWithDetails), args.Error(1)
}

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName)
	return args.Get(0).([]*scheduler.JobWithDetails), args.Error(1)
}

type mockScheduler struct {
	mock.Mock
}

func (ms *mockScheduler) GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	args := ms.Called(ctx, t, criteria, jobCron)
	return args.Get(0).([]*scheduler.JobRunStatus), args.Error(1)
}

func (ms *mockScheduler) DeployJobs(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	args := ms.Called(ctx, t, jobs)
	return args.Error(0)
}

func (ms *mockScheduler) ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error) {
	args := ms.Called(ctx, t)
	return args.Get(0).([]string), args.Error(1)
}

func (ms *mockScheduler) DeleteJobs(ctx context.Context, t tenant.Tenant, jobsToDelete []string) error {
	args := ms.Called(ctx, t, jobsToDelete)
	return args.Error(0)
}

func TestJobRunService(t *testing.T) {
	ctx := context.Background()
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	jobName := scheduler.JobName("sample_select")

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
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			criteria := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			sch := new(mockScheduler)
			sch.On("GetJobRuns", ctx, tnnt, criteria, jobCron).Return([]*scheduler.JobRunStatus{}, nil)
			defer sch.AssertExpectations(t)
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, nil, nil, sch, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.Nil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should able to get job runs when scheduler returns valid response", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			runsFromScheduler, err := mockGetJobRuns(5, startDate, jobWithDetails.Schedule.Interval, scheduler.StateSuccess.String())
			if err != nil {
				t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
			}
			runsFromSchFor3days, err := mockGetJobRuns(3, startDate, jobWithDetails.Schedule.Interval, scheduler.StateSuccess.String())
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for success state %v", err)
			}
			expPendingRuns, err := mockGetJobRuns(2, startDate.Add(time.Hour*24*3), jobWithDetails.Schedule.Interval, scheduler.StatePending.String())
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for pending state %v", err)
			}
			type cases struct {
				description    string
				input          *scheduler.JobRunsCriteria
				job            scheduler.JobWithDetails
				runs           []*scheduler.JobRunStatus
				expectedResult []*scheduler.JobRunStatus
			}
			for _, scenario := range []cases{
				{
					description: "filtering based on success",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateSuccess.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on failed",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateFailed.String()},
					},
					job:            jobWithDetails,
					expectedResult: nil,
				},
				{
					description: "no filterRuns applied",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on pending",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StatePending.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: nil,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on pending status",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StatePending.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: expPendingRuns,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on success status",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateSuccess.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: runsFromSchFor3days,
				},
				{
					description: "when some job instances are not started by scheduler and no filterRuns applied",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: append(runsFromSchFor3days, expPendingRuns...),
				},
			} {
				t.Run(scenario.description, func(t *testing.T) {
					sch := new(mockScheduler)
					sch.On("GetJobRuns", ctx, tnnt, scenario.input, jobCron).Return(scenario.runs, nil)
					defer sch.AssertExpectations(t)
					jobRepo := new(JobRepository)
					jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
					defer jobRepo.AssertExpectations(t)
					runService := service.NewJobRunService(nil,
						jobRepo, nil, nil,
						sch, nil, nil)
					returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, scenario.input)
					assert.Nil(t, err)
					assert.Equal(t, scenario.expectedResult, returnedRuns)
				})
			}
		})
		t.Run("should not able to get job runs when invalid date range is given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate.Add(-time.Hour * 24 * 2),
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil, jobRepo, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when invalid cron interval present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "invalid interval",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil, jobRepo, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when no cron interval present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil, jobRepo, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when no start date present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					//StartDate: startDate.Add(-time.Hour * 24),
					EndDate:  nil,
					Interval: "0 12 * * *",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)
			runService := service.NewJobRunService(nil, jobRepo, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.NotNil(t, err)
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should not able to get job runs when scheduler returns an error", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			criteria := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			sch := new(mockScheduler)
			sch.On("GetJobRuns", ctx, tnnt, criteria, jobCron).Return([]*scheduler.JobRunStatus{}, errors.InvalidArgument(scheduler.EntityJobRun, "failed: due to invalid URL"))
			defer sch.AssertExpectations(t)
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil, jobRepo, nil, nil, sch, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.NotNil(t, err, errors.InvalidArgument(scheduler.EntityJobRun, "failed: due to invalid URL"))
			assert.Nil(t, nil, returnedRuns)
		})
		t.Run("should able to get job runs when only last run is required", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			criteria := &scheduler.JobRunsCriteria{
				Name:        "sample_select",
				OnlyLastRun: true,
			}

			runs := []*scheduler.JobRunStatus{
				{
					State:       scheduler.StateSuccess,
					ScheduledAt: endDate,
				},
			}

			sch := new(mockScheduler)
			sch.On("GetJobRuns", ctx, tnnt, criteria, jobCron).Return(runs, nil)
			defer sch.AssertExpectations(t)
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil, jobRepo, nil, nil, sch, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.Nil(t, err)
			assert.Equal(t, runs, returnedRuns)
		})
	})
}

func mockGetJobRuns(afterDays int, date time.Time, interval, status string) ([]*scheduler.JobRunStatus, error) {
	var expRuns []*scheduler.JobRunStatus
	schSpec, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return expRuns, err
	}
	nextStart := schSpec.Next(date.Add(-time.Second * 1))
	for i := 0; i < afterDays; i++ {
		expRuns = append(expRuns, &scheduler.JobRunStatus{
			State:       scheduler.State(status),
			ScheduledAt: nextStart,
		})
		nextStart = schSpec.Next(nextStart)
	}
	return expRuns, nil
}
