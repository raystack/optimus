package job_run_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
)

func TestJob(t *testing.T) {
	t.Run("GroupJobsByTenant", func(t *testing.T) {
		t1, _ := tenant.NewTenant("proj", "ns1")
		t2, _ := tenant.NewTenant("proj", "ns1")
		t3, _ := tenant.NewTenant("proj", "ns2")
		job1 := job_run.JobWithDetails{
			Name: "job1",
			Job: &job_run.Job{
				Tenant: t1,
			},
		}
		job2 := job_run.JobWithDetails{
			Name: "job2",
			Job: &job_run.Job{
				Tenant: t2,
			},
		}

		job3 := job_run.JobWithDetails{
			Name: "job3",
			Job: &job_run.Job{
				Tenant: t1,
			},
		}
		job4 := job_run.JobWithDetails{
			Name: "job4",
			Job: &job_run.Job{
				Tenant: t3,
			},
		}

		group := job_run.GroupJobsByTenant(
			[]*job_run.JobWithDetails{
				&job1, &job2, &job3, &job4,
			})

		assert.Equal(t, 2, len(group))
		assert.Equal(t, 3, len(group[t1]))
		assert.Equal(t, 1, len(group[t3]))
	})
}
