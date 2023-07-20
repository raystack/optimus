package service

import (
	"context"

	"go.opentelemetry.io/otel"

	"github.com/raystack/optimus/core/scheduler"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/errors"
)

func (s *JobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error {
	spanCtx, span := otel.Tracer("optimus").Start(ctx, "UploadToScheduler")
	defer span.End()

	me := errors.NewMultiError("errorInUploadToScheduler")
	allJobsWithDetails, err := s.jobRepo.GetAll(spanCtx, projectName)
	me.Append(err)
	if allJobsWithDetails == nil {
		return me.ToErr()
	}
	span.AddEvent("got all the jobs to upload")

	err = s.priorityResolver.Resolve(spanCtx, allJobsWithDetails)
	if err != nil {
		s.l.Error("error resolving priority: %s", err)
		me.Append(err)
		return me.ToErr()
	}
	span.AddEvent("done with priority resolution")

	jobGroupByTenant := scheduler.GroupJobsByTenant(allJobsWithDetails)
	for t, jobs := range jobGroupByTenant {
		span.AddEvent("uploading job specs")
		if err = s.deployJobsPerNamespace(spanCtx, t, jobs); err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed", t.NamespaceName().String(), t.ProjectName().String())
		}
		me.Append(err)

		span.AddEvent("uploading job metrics")
	}
	return me.ToErr()
}

func (s *JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		s.l.Error("error deploying jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s *JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	// get all stored job names
	schedulerJobNames, err := s.scheduler.ListJobs(ctx, t)
	if err != nil {
		s.l.Error("error listing jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
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

func (s *JobRunService) UploadJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate, toDelete []string) (err error) {
	me := errors.NewMultiError("errorInUploadJobs")

	if len(toUpdate) > 0 {
		if err = s.resolveAndDeployJobs(ctx, tnnt, toUpdate); err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed %d jobs", tnnt.NamespaceName().String(),
				tnnt.ProjectName().String(), len(toUpdate))
		}
		me.Append(err)
	}

	if len(toDelete) > 0 {
		if err = s.scheduler.DeleteJobs(ctx, tnnt, toDelete); err == nil {
			s.l.Info("deleted %s jobs on project: %s", len(toDelete), tnnt.ProjectName())
		}
		me.Append(err)
	}

	return me.ToErr()
}

func (s *JobRunService) resolveAndDeployJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate []string) error {
	allJobsWithDetails, err := s.jobRepo.GetJobs(ctx, tnnt.ProjectName(), toUpdate)
	if err != nil || allJobsWithDetails == nil {
		return err
	}

	if err := s.priorityResolver.Resolve(ctx, allJobsWithDetails); err != nil {
		s.l.Error("error priority resolving jobs: %s", err)
		return err
	}

	return s.scheduler.DeployJobs(ctx, tnnt, allJobsWithDetails)
}
