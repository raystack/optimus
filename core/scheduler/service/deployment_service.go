package service

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/telemetry"
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
		setJobMetric(t, jobs)
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
