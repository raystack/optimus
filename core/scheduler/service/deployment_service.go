package service

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

func (s JobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error {
	multiError := errors.NewMultiError("errorInUploadToScheduler")
	allJobsWithDetails, err := s.jobRepo.GetAll(ctx, projectName)
	multiError.Append(err)
	if allJobsWithDetails == nil {
		return errors.MultiToError(multiError)
	}
	err = s.priorityResolver.Resolve(ctx, allJobsWithDetails)
	if err != nil {
		multiError.Append(err)
		return errors.MultiToError(multiError)
	}
	jobGroupByTenant := scheduler.GroupJobsByTenant(allJobsWithDetails)
	for t, jobs := range jobGroupByTenant {
		if err = s.deployJobsPerNamespace(ctx, t, jobs); err == nil {
			s.l.Debug(fmt.Sprintf("[success] namespace: %s, project: %s, deployed", t.NamespaceName().String(), t.ProjectName().String()))
		}
		multiError.Append(err)
	}
	return multiError.ToErr()
}

func (s JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	// get all stored job names
	schedulerJobNames, err := s.scheduler.ListJobs(ctx, t)
	if err != nil {
		return err
	}
	jobNamesMap := make(map[string]struct{})
	for _, job := range jobs {
		jobNamesMap[job.Name.String()] = struct{}{}
	}
	var jobsToDelete []string

	for _, schedulerJobName := range schedulerJobNames {
		if _, ok := jobNamesMap[schedulerJobName]; !ok {
			jobsToDelete = append(jobsToDelete, schedulerJobName)
		}
	}
	return s.scheduler.DeleteJobs(ctx, t, jobsToDelete)
}
