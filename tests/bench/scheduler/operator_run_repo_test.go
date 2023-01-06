//go:build !unit_test
// +build !unit_test

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceJob "github.com/odpf/optimus/core/job"
	serviceScheduler "github.com/odpf/optimus/core/scheduler"
	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoJob "github.com/odpf/optimus/internal/store/postgres/job"
	repoScheduler "github.com/odpf/optimus/internal/store/postgres/scheduler"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkOperatorRunRepository(b *testing.B) {
	ctx := context.Background()

	proj, err := serviceTenant.NewProject("test-proj",
		map[string]string{
			"bucket":                            "gs://some_folder-2",
			serviceTenant.ProjectSchedulerHost:  "host",
			serviceTenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(b, err)
	tnnt, err := serviceTenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	dbSetup := func() *pgxpool.Pool {
		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		projRepo := repoTenant.NewProjectRepository(pool)
		if err := projRepo.Save(ctx, proj); err != nil {
			panic(err)
		}

		namespaceRepo := repoTenant.NewNamespaceRepository(pool)
		if err := namespaceRepo.Save(ctx, namespace); err != nil {
			panic(err)
		}
		return pool
	}

	b.Run("CreateOperatorRun", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(jobName.String())
		assert.NoError(b, err)
		scheduledAt := time.Now()
		err = schedulerJobRunRepo.Create(ctx, tnnt, jobNameForRun, scheduledAt, int64(time.Second))
		assert.NoError(b, err)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForRun, scheduledAt)
		assert.NotNil(b, storedJobRun)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			actualError := schedulerOperatorRunRepo.CreateOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID, time.Now())
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetOperatorRun", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(jobName.String())
		assert.NoError(b, err)
		scheduledAt := time.Now()
		err = schedulerJobRunRepo.Create(ctx, tnnt, jobNameForRun, scheduledAt, int64(time.Second))
		assert.NoError(b, err)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForRun, scheduledAt)
		assert.NotNil(b, storedJobRun)
		assert.NoError(b, err)

		maxNumberOfOperatorRun := 50
		for i := 0; i < maxNumberOfOperatorRun; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			err = schedulerOperatorRunRepo.CreateOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID, time.Now())
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			operatorRunIdx := i % maxNumberOfOperatorRun
			name := fmt.Sprintf("operator_for_test_%d", operatorRunIdx)
			actualOperatorRun, actualError := schedulerOperatorRunRepo.GetOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID)
			assert.NotNil(b, actualOperatorRun)
			assert.NoError(b, actualError)
		}
	})

	b.Run("UpdateOperatorRun", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(jobName.String())
		assert.NoError(b, err)
		scheduledAt := time.Now()
		err = schedulerJobRunRepo.Create(ctx, tnnt, jobNameForRun, scheduledAt, int64(time.Second))
		assert.NoError(b, err)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForRun, scheduledAt)
		assert.NotNil(b, storedJobRun)
		assert.NoError(b, err)

		maxNumberOfOperatorRun := 50
		operatorStartTime := time.Now()
		for i := 0; i < maxNumberOfOperatorRun; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			err = schedulerOperatorRunRepo.CreateOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID, operatorStartTime)
			assert.NoError(b, err)
		}

		operatorRuns := make([]*serviceScheduler.OperatorRun, maxNumberOfOperatorRun)
		for i := 0; i < maxNumberOfOperatorRun; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			storedOperatorRun, err := schedulerOperatorRunRepo.GetOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID)
			assert.NotNil(b, storedOperatorRun)
			assert.NoError(b, err)

			operatorRuns[i] = storedOperatorRun
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			operatorRunIdx := i % maxNumberOfOperatorRun
			operatorRun := operatorRuns[operatorRunIdx]
			actualError := schedulerOperatorRunRepo.UpdateOperatorRun(ctx, serviceScheduler.OperatorTask, operatorRun.ID, operatorStartTime, serviceScheduler.StateAccepted)
			assert.NoError(b, actualError)
		}
	})
}
