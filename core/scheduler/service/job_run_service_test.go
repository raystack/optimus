package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
)

func TestJobRunService(t *testing.T) {
	ctx := context.Background()
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	jobName := scheduler.JobName("sample_select")
	todayDate := time.Now()

	t.Run("UpdateJobState", func(t *testing.T) {
		tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

		t.Run("registerNewJobRun", func(t *testing.T) {
			t.Run("should return error on JobStartEvent if GetJobDetails fails", func(t *testing.T) {
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(nil, fmt.Errorf("some error"))
				defer jobRepo.AssertExpectations(t)

				runService := service.NewJobRunService(nil,
					jobRepo, nil, nil, nil, nil, nil)

				event := scheduler.Event{
					JobName: jobName,
					Tenant:  tnnt,
					Type:    scheduler.JobStartEvent,
					Values:  map[string]any{},
				}
				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "some error")
			})
			t.Run("should return error on JobStartEvent if job.SLADuration fails, due to wrong duration format", func(t *testing.T) {
				JobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "wrong duration format",
							},
						},
					},
				}
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&JobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				runService := service.NewJobRunService(nil,
					jobRepo, nil, nil, nil, nil, nil)

				event := scheduler.Event{
					JobName: jobName,
					Tenant:  tnnt,
					Type:    scheduler.JobStartEvent,
					Values:  map[string]any{},
				}

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "failed to parse sla_miss duration wrong duration format: time: invalid duration \"wrong duration format\"")
			})
			t.Run("should create job_run row on JobStartEvent", func(t *testing.T) {
				JobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "2h45m",
							},
						},
					},
				}
				slaDefinitionInSec, err := JobWithDetails.SLADuration()
				assert.Nil(t, err)

				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				event := scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobStartEvent,
					EventTime:      time.Time{},
					OperatorName:   "job_start_event",
					JobScheduledAt: scheduledAtTimeStamp,
					Values:         map[string]any{},
				}

				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&JobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil)
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(nil,
					jobRepo, jobRunRepo, nil, nil, nil, nil)

				err = runService.UpdateJobState(ctx, event)
				assert.Nil(t, err)
			})
		})

		t.Run("updateJobRun", func(t *testing.T) {
			t.Run("should update job_run row on JobSuccessEvent, when no error in format etc", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				endTime := eventTime
				event := scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobSuccessEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values: map[string]any{
						"status": "success",
					},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: todayDate,
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
				jobRunRepo.On("Update", ctx, jobRun.ID, endTime, event.Values["status"].(string)).Return(nil)
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(nil,
					nil, jobRunRepo, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.Nil(t, err)
			})
			t.Run("should create and update job_run row on JobSuccessEvent, when job_run row does not exist already", func(t *testing.T) {
				JobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "2h45m",
							},
						},
					},
				}
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&JobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				endTime := eventTime
				event := scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobFailEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values: map[string]any{
						"status": "success",
					},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: time.Now(),
				}
				slaDefinitionInSec, _ := JobWithDetails.SLADuration()

				t.Run("scenario, return error when, GetByScheduledAt return errors other than not found", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("some random error")).Once()
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(nil,
						jobRepo, jobRunRepo, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "some random error")
				})
				t.Run("scenario, return error when, unable to create job run", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found")).Once()
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(fmt.Errorf("unable to create job run")).Once()
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(nil,
						jobRepo, jobRunRepo, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "unable to create job run")
				})
				t.Run("scenario, return error when, despite successful creation getByScheduledAt still fails", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found"))
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(nil,
						jobRepo, jobRunRepo, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "not found for entity jobRun: job run not found")
				})
				t.Run("scenario should successfully register new job run row", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found")).Once()
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil).Once()
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil).Once()
					jobRunRepo.On("Update", ctx, jobRun.ID, endTime, event.Values["status"].(string)).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(nil,
						jobRepo, jobRunRepo, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.Nil(t, err)

				})
			})
		})

		t.Run("createOperatorRun", func(t *testing.T) {

		})
		t.Run("updateOperatorRun", func(t *testing.T) {

		})
	})

	t.Run("JobRunInput", func(t *testing.T) {
		t.Run("should return error if getJob fails", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetJob", ctx, projName, jobName).Return(&scheduler.Job{}, fmt.Errorf("some error"))
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, nil, nil, nil, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, scheduler.RunConfig{})
			assert.Nil(t, executorInput)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("should get jobRunByScheduledAt if job run id is not given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			executedAt := todayDate.Add(time.Hour * 23 * -1)
			startTime := executedAt
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    scheduler.JobRunID{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJob", ctx, projName, jobName).
				Return(&job, nil)
			defer jobRepo.AssertExpectations(t)

			jobRun := scheduler.JobRun{

				JobName:   jobName,
				Tenant:    tnnt,
				StartTime: startTime,
			}
			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, someScheduleTime).
				Return(&jobRun, nil)
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &job, runConfig, executedAt).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, jobRunRepo, nil, nil, nil, jobInputCompiler)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should use GetByID if job run id is given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			executedAt := todayDate.Add(time.Hour * 23 * -1)
			startTime := executedAt
			JobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    JobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJob", ctx, projName, jobName).
				Return(&job, nil)
			defer jobRepo.AssertExpectations(t)

			jobRun := scheduler.JobRun{
				JobName:   jobName,
				Tenant:    tnnt,
				StartTime: startTime,
			}
			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, JobRunID).
				Return(&jobRun, nil)
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &job, runConfig, executedAt).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, jobRunRepo, nil, nil, nil, jobInputCompiler)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should handle if job run is not found , and fallback to execution time being schedule time", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			jobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    jobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJob", ctx, projName, jobName).
				Return(&job, nil)
			defer jobRepo.AssertExpectations(t)

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, jobRunID).
				Return(&scheduler.JobRun{}, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()))
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &job, runConfig, someScheduleTime).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, jobRunRepo, nil, nil, nil, jobInputCompiler)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should return error if get job run fails", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			jobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    jobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJob", ctx, projName, jobName).
				Return(&job, nil)
			defer jobRepo.AssertExpectations(t)

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, jobRunID).
				Return(&scheduler.JobRun{}, fmt.Errorf("some error other than not found error "))
			defer jobRunRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, jobRunRepo, nil, nil, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Nil(t, executorInput)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error other than not found error ")
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
			assert.EqualError(t, err, "invalid date range")
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
			assert.EqualError(t, err, "unable to parse job cron interval expected exactly 5 fields, found 2: [invalid interval]")
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
			assert.EqualError(t, err, "job schedule interval not found")
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
			assert.EqualError(t, err, "job schedule startDate not found in job fetched from DB")
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
			assert.NotNil(t, err)
			assert.Error(t, err, errors.InvalidArgument(scheduler.EntityJobRun, "failed: due to invalid URL"))
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

type mockJobInputCompiler struct {
	mock.Mock
}

func (m *mockJobInputCompiler) Compile(ctx context.Context, job *scheduler.Job, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error) {
	args := m.Called(ctx, job, config, executedAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.ExecutorInput), args.Error(1)
}

type mockJobRunRepository struct {
	mock.Mock
}

func (m *mockJobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) GetByScheduledAt(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	args := m.Called(ctx, tenant, name, scheduledAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	args := m.Called(ctx, tenant, name, scheduledAt, slaDefinitionInSec)
	return args.Error(0)
}

func (m *mockJobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus string) error {
	args := m.Called(ctx, jobRunID, endTime, jobRunStatus)
	return args.Error(0)
}

type JobRepository struct {
	mock.Mock
}

func (j *JobRepository) GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	args := j.Called(ctx, name, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.Job), args.Error(1)
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobWithDetails), args.Error(1)
}

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobWithDetails), args.Error(1)
}

type mockScheduler struct {
	mock.Mock
}

func (ms *mockScheduler) GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	args := ms.Called(ctx, t, criteria, jobCron)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
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
