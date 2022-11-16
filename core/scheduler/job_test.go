package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
)

func TestJob(t *testing.T) {
	t.Run("GroupJobsByTenant", func(t *testing.T) {
		t1, _ := tenant.NewTenant("proj", "ns1")
		t2, _ := tenant.NewTenant("proj", "ns1")
		t3, _ := tenant.NewTenant("proj", "ns2")
		job1 := scheduler.JobWithDetails{
			Name: "job1",
			Job: &scheduler.Job{
				Tenant: t1,
			},
		}
		job2 := scheduler.JobWithDetails{
			Name: "job2",
			Job: &scheduler.Job{
				Tenant: t2,
			},
		}

		job3 := scheduler.JobWithDetails{
			Name: "job3",
			Job: &scheduler.Job{
				Tenant: t1,
			},
		}
		job4 := scheduler.JobWithDetails{
			Name: "job4",
			Job: &scheduler.Job{
				Tenant: t3,
			},
		}

		group := scheduler.GroupJobsByTenant(
			[]*scheduler.JobWithDetails{
				&job1, &job2, &job3, &job4,
			})

		assert.Equal(t, 2, len(group))
		assert.Equal(t, 3, len(group[t1]))
		assert.Equal(t, 1, len(group[t3]))
	})
}
