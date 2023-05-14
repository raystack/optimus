package resolver

import (
	"context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
)

type SimpleResolver struct{}

func NewSimpleResolver() *SimpleResolver {
	return &SimpleResolver{}
}

func (SimpleResolver) Resolve(_ context.Context, details []*scheduler.JobWithDetails) error { // nolint:unparam
	for _, job := range details {
		priority := MaxPriorityWeight - numberOfUpstreams(job.Upstreams, job.Job.Tenant)*PriorityWeightGap
		job.Priority = priority
	}
	return nil
}

func numberOfUpstreams(upstream scheduler.Upstreams, tnnt tenant.Tenant) int {
	count := 0
	for _, u := range upstream.UpstreamJobs {
		if u.Tenant == tnnt {
			count++
		}
	}
	return count
}
