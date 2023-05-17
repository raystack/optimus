package service

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/telemetry"
)

func setJobMetric(t tenant.Tenant, jobs []*scheduler.JobWithDetails) {
	telemetry.NewGauge("total_number_of_job", map[string]string{
		"project":   t.ProjectName().String(),
		"namespace": t.NamespaceName().String(),
	}).Set(float64(len(jobs)))

	// this can be greatly simplified using a db query
	type counter struct {
		Inferred int
		Static   int
	}
	externalUpstreamCountMap := map[string]*counter{}
	for _, job := range jobs {
		for _, upstream := range job.Upstreams.UpstreamJobs {
			if upstream.External {
				if _, ok := externalUpstreamCountMap[upstream.Host]; !ok {
					externalUpstreamCountMap[upstream.Host] = &counter{}
				}
				if upstream.Type == scheduler.UpstreamTypeStatic {
					externalUpstreamCountMap[upstream.Host].Static++
				} else {
					externalUpstreamCountMap[upstream.Host].Inferred++
				}
			}
		}
	}

	for externalUpstream, counter := range externalUpstreamCountMap {
		if counter.Static > 0 {
			telemetry.NewGauge("total_external_upstream_references", map[string]string{
				"project":   t.ProjectName().String(),
				"namespace": t.NamespaceName().String(),
				"host":      externalUpstream,
				"type":      scheduler.UpstreamTypeStatic,
			}).Set(float64(counter.Static))
		}

		if counter.Inferred > 0 {
			telemetry.NewGauge("total_external_upstream_references", map[string]string{
				"project":   t.ProjectName().String(),
				"namespace": t.NamespaceName().String(),
				"host":      externalUpstream,
				"type":      scheduler.UpstreamTypeInferred,
			}).Set(float64(counter.Inferred))
		}
	}
}

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
		me.Append(err)
		return me.ToErr()
	}
	span.AddEvent("done with priority resolution")

	jobGroupByTenant := scheduler.GroupJobsByTenant(allJobsWithDetails)
	for t, jobs := range jobGroupByTenant {
		span.AddEvent("uploading job specs")
		if err = s.deployJobsPerNamespace(spanCtx, t, jobs); err == nil {
			s.l.Debug(fmt.Sprintf("[success] namespace: %s, project: %s, deployed", t.NamespaceName().String(), t.ProjectName().String()))
		}
		me.Append(err)

		span.AddEvent("uploading job metrics")
		setJobMetric(t, jobs)
	}
	return me.ToErr()
}

func (s *JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s *JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
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

func (s *JobRunService) UploadJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate, toDelete []string) (err error) {
	me := errors.NewMultiError("errorInUploadJobs")

	if len(toUpdate) > 0 {
		if err = s.resolveAndDeployJobs(ctx, tnnt, toUpdate); err == nil {
			s.l.Info(fmt.Sprintf("[success] namespace: %s, project: %s, deployed %d jobs", tnnt.NamespaceName().String(),
				tnnt.ProjectName().String(), len(toUpdate)))
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

	s.l.Info("got jobs to upload to scheduler")
	if err := s.priorityResolver.Resolve(ctx, allJobsWithDetails); err != nil {
		return err
	}

	return s.scheduler.DeployJobs(ctx, tnnt, allJobsWithDetails)
}
