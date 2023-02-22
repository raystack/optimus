package service

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

var (
	totalJobsMetricMap             = map[string]prometheus.Gauge{}
	totalExternalUpstreamMetricMap = map[string]prometheus.Gauge{}
)

func setJobMetric(t tenant.Tenant, jobs []*scheduler.JobWithDetails) {
	totalJobsMetricKey := fmt.Sprintf("total_number_of_job/%s/%s", t.ProjectName().String(), t.NamespaceName().String())
	if _, ok := totalJobsMetricMap[totalJobsMetricKey]; !ok {
		totalJobsMetricMap[totalJobsMetricKey] = promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "total_number_of_job",
			ConstLabels: map[string]string{"project": t.ProjectName().String(), "namespace": t.NamespaceName().String()},
		})
	}
	totalJobsMetricMap[totalJobsMetricKey].Set(float64(len(jobs)))

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
		totalStaticExternalUpstreamMetricKey := fmt.Sprintf("total_external_upstream_references/%s/%s/%s/static", t.ProjectName().String(), t.NamespaceName().String(), externalUpstream)
		if _, ok := totalExternalUpstreamMetricMap[totalStaticExternalUpstreamMetricKey]; !ok {
			totalExternalUpstreamMetricMap[totalStaticExternalUpstreamMetricKey] = promauto.NewGauge(prometheus.GaugeOpts{
				Name: "total_external_upstream_references",
				ConstLabels: map[string]string{
					"project":   t.ProjectName().String(),
					"namespace": t.NamespaceName().String(),
					"host":      externalUpstream,
					"type":      scheduler.UpstreamTypeStatic,
				},
			})
		}
		totalExternalUpstreamMetricMap[totalStaticExternalUpstreamMetricKey].Set(float64(counter.Static))

		totalInferredExternalUpstreamMetricKey := fmt.Sprintf("total_external_upstream_references/%s/%s/%s/inferred", t.ProjectName().String(), t.NamespaceName().String(), externalUpstream)
		if _, ok := totalExternalUpstreamMetricMap[totalInferredExternalUpstreamMetricKey]; !ok {
			totalExternalUpstreamMetricMap[totalInferredExternalUpstreamMetricKey] = promauto.NewGauge(prometheus.GaugeOpts{
				Name: "total_external_upstream_references",
				ConstLabels: map[string]string{
					"project":   t.ProjectName().String(),
					"namespace": t.NamespaceName().String(),
					"host":      externalUpstream,
					"type":      scheduler.UpstreamTypeInferred,
				},
			})
		}
		totalExternalUpstreamMetricMap[totalInferredExternalUpstreamMetricKey].Set(float64(counter.Inferred))
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
