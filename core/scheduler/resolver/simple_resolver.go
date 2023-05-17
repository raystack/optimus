package resolver

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

const (
	// maxPriorityWeight - is the maximus weight a DAG will be given.
	maxPriorityWeight = 10000

	// priorityWeightGap - while giving weights to the DAG, what's the GAP
	// do we want to consider. PriorityWeightGap = 1 means, weights will be 1, 2, 3 etc.
	priorityWeightGap = 10
)

type SimpleResolver struct{}

func NewSimpleResolver() *SimpleResolver {
	return &SimpleResolver{}
}

func (SimpleResolver) Resolve(_ context.Context, details []*scheduler.JobWithDetails) error { // nolint:unparam
	for _, job := range details {
		priority := maxPriorityWeight - numberOfUpstreams(job.Upstreams, job.Job.Tenant)*priorityWeightGap
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
