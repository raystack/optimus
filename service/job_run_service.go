package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/optimus/models"
)

type JobRunService interface {
	// GetJobRunList returns all the job based given status and date range
	GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error)
}

type jobRunService struct {
	scheduler models.SchedulerUnit
}

func (s *jobRunService) GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error) {
	var jobRuns []models.JobRun

	interval := jobSpec.Schedule.Interval
	if interval == "" {
		return jobRuns, errors.New("job schedule interval not found")
	}
	// jobCron
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to parse job cron internval %w", err)
	}

	if jobQuery.OnlyLastRun {
		return s.scheduler.GetJobRuns(ctx, projectSpec, jobQuery, jobCron)
	}
	// validate job query
	err = validateJobQuery(jobQuery, jobSpec)
	if err != nil {
		return jobRuns, err
	}
	// get expected runs StartDate and EndDate inclusive
	expectedRuns := getExpectedRuns(jobCron, jobQuery.StartDate, jobQuery.EndDate)

	// call to airflow for get runs
	actualRuns, err := s.scheduler.GetJobRuns(ctx, projectSpec, jobQuery, jobCron)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to get job runs from airflow %w", err)
	}
	// mergeRuns
	totalRuns := mergeRuns(expectedRuns, actualRuns)

	// filterRuns
	result := filterRuns(totalRuns, createFilterSet(jobQuery.Filter))

	return result, nil
}

func NewJobRunService(scheduler models.SchedulerUnit) *jobRunService {
	return &jobRunService{
		scheduler: scheduler,
	}
}

func validateJobQuery(jobQuery *models.JobQuery, jobSpec models.JobSpec) error {
	jobStartDate := jobSpec.Schedule.StartDate
	if jobStartDate.IsZero() {
		return errors.New("job schedule startDate not found in job fetched from DB")
	}
	givenStartDate := jobQuery.StartDate
	givenEndDate := jobQuery.EndDate

	if givenStartDate.Before(jobStartDate) || givenEndDate.Before(jobStartDate) {
		return errors.New("invalid date range")
	}

	return nil
}

func getExpectedRuns(spec *cron.ScheduleSpec, startTime, endTime time.Time) []models.JobRun {
	var jobRuns []models.JobRun
	start := spec.Next(startTime.Add(-time.Second * 1))
	end := endTime
	exit := spec.Next(end)
	for !start.Equal(exit) {
		jobRuns = append(jobRuns, models.JobRun{
			Status:      models.RunStatePending,
			ScheduledAt: start,
		})
		start = spec.Next(start)
	}
	return jobRuns
}

func mergeRuns(expected, actual []models.JobRun) []models.JobRun {
	var mergeRuns []models.JobRun
	m := actualRunMap(actual)
	for _, exp := range expected {
		if act, ok := m[exp.ScheduledAt.UTC().String()]; ok {
			mergeRuns = append(mergeRuns, act)
		} else {
			mergeRuns = append(mergeRuns, exp)
		}
	}
	return mergeRuns
}

func actualRunMap(runs []models.JobRun) map[string]models.JobRun {
	m := map[string]models.JobRun{}
	for _, v := range runs {
		m[v.ScheduledAt.UTC().String()] = v
	}
	return m
}

func filterRuns(runs []models.JobRun, filter map[string]struct{}) []models.JobRun {
	var filteredRuns []models.JobRun
	if len(filter) == 0 {
		return runs
	}
	for _, v := range runs {
		if _, ok := filter[v.Status.String()]; ok {
			filteredRuns = append(filteredRuns, v)
		}
	}
	return filteredRuns
}

func createFilterSet(filter []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, v := range filter {
		m[models.JobRunState(v).String()] = struct{}{}
	}
	return m
}
