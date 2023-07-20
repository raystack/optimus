package resolver

import "github.com/raystack/optimus/core/job"

func mergeUpstreams(upstreamGroups ...[]*job.Upstream) []*job.Upstream {
	var allUpstreams []*job.Upstream
	for _, group := range upstreamGroups {
		allUpstreams = append(allUpstreams, group...)
	}
	return allUpstreams
}
