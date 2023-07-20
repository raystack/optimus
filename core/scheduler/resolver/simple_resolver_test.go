package resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/scheduler"
	"github.com/raystack/optimus/core/scheduler/resolver"
	"github.com/raystack/optimus/core/tenant"
)

func TestSimpleResolver(t *testing.T) {
	ctx := context.Background()
	tnnt1, _ := tenant.NewTenant("test-proj", "test-ns")

	t.Run("returns max priority for root node", func(t *testing.T) {
		j1 := &scheduler.JobWithDetails{
			Name: scheduler.JobName("RootNode"),
			Job:  &scheduler.Job{Tenant: tnnt1},
			Upstreams: scheduler.Upstreams{
				UpstreamJobs: nil,
			},
		}

		s1 := resolver.SimpleResolver{}
		err := s1.Resolve(ctx, []*scheduler.JobWithDetails{j1})
		assert.NoError(t, err)
		assert.Equal(t, 10000, j1.Priority)
	})
	t.Run("returns max priority for upstream not in same tenant", func(t *testing.T) {
		tnnt2, _ := tenant.NewTenant("proj2", "namespace2")
		upstream := &scheduler.JobUpstream{
			JobName: "upstreamInOtherTenant",
			Tenant:  tnnt2,
		}

		j1 := &scheduler.JobWithDetails{
			Name: scheduler.JobName("RootNode"),
			Job:  &scheduler.Job{Tenant: tnnt1},
			Upstreams: scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{upstream},
			},
		}

		s1 := resolver.SimpleResolver{}
		err := s1.Resolve(ctx, []*scheduler.JobWithDetails{j1})
		assert.NoError(t, err)
		assert.Equal(t, 10000, j1.Priority)
	})
	t.Run("returns priority for leaf based on upstream", func(t *testing.T) {
		upstream1 := &scheduler.JobUpstream{
			JobName: "upstream1",
			Tenant:  tnnt1,
		}

		upstream2 := &scheduler.JobUpstream{
			JobName: "upstream2",
			Tenant:  tnnt1,
		}

		j1 := &scheduler.JobWithDetails{
			Name: scheduler.JobName("RootNode"),
			Job:  &scheduler.Job{Tenant: tnnt1},
			Upstreams: scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{upstream1, upstream2},
			},
		}

		s1 := resolver.SimpleResolver{}
		err := s1.Resolve(ctx, []*scheduler.JobWithDetails{j1})
		assert.NoError(t, err)
		assert.Equal(t, 9980, j1.Priority)
	})
}
