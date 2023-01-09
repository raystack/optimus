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
	const maxNumberOfOperatorRuns = 64

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}
	project, err := serviceTenant.NewProject("project_for_test", config)
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("namespace_for_test", project.Name(), config)
	assert.NoError(b, err)
	tnnt, err := serviceTenant.NewTenant(project.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	ctx := context.Background()

	dbSetup := func(b *testing.B) *pgxpool.Pool {
		b.Helper()

		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		projectRepo := repoTenant.NewProjectRepository(pool)
		err := projectRepo.Save(ctx, project)
		assert.NoError(b, err)

		namespaceRepo := repoTenant.NewNamespaceRepository(pool)
		err = namespaceRepo.Save(ctx, namespace)
		assert.NoError(b, err)

		return pool
	}

	b.Run("CreateOperatorRun", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(job.GetName())
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
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(job.GetName())
		assert.NoError(b, err)

		scheduledAt := time.Now()

		err = schedulerJobRunRepo.Create(ctx, tnnt, jobNameForRun, scheduledAt, int64(time.Second))
		assert.NoError(b, err)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForRun, scheduledAt)
		assert.NotNil(b, storedJobRun)
		assert.NoError(b, err)

		operatorRunNames := make([]string, maxNumberOfOperatorRuns)
		for i := 0; i < maxNumberOfOperatorRuns; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			err = schedulerOperatorRunRepo.CreateOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID, time.Now())
			assert.NoError(b, err)

			operatorRunNames[i] = name
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			operatorRunIdx := i % maxNumberOfOperatorRuns
			name := operatorRunNames[operatorRunIdx]

			actualOperatorRun, actualError := schedulerOperatorRunRepo.GetOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID)
			assert.NotNil(b, actualOperatorRun)
			assert.NoError(b, actualError)
		}
	})

	b.Run("UpdateOperatorRun", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)
		schedulerOperatorRunRepo := repoScheduler.NewOperatorRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForRun, err := serviceScheduler.JobNameFrom(job.GetName())
		assert.NoError(b, err)

		scheduledAt := time.Now()

		err = schedulerJobRunRepo.Create(ctx, tnnt, jobNameForRun, scheduledAt, int64(time.Second))
		assert.NoError(b, err)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForRun, scheduledAt)
		assert.NotNil(b, storedJobRun)
		assert.NoError(b, err)

		operatorRunNames := make([]string, maxNumberOfOperatorRuns)
		operatorStartTime := time.Now()
		for i := 0; i < maxNumberOfOperatorRuns; i++ {
			name := fmt.Sprintf("operator_for_test_%d", i)
			err = schedulerOperatorRunRepo.CreateOperatorRun(ctx, name, serviceScheduler.OperatorTask, storedJobRun.ID, operatorStartTime)
			assert.NoError(b, err)

			operatorRunNames[i] = name
		}

		storedOperatorRuns := make([]*serviceScheduler.OperatorRun, maxNumberOfOperatorRuns)
		for i := 0; i < maxNumberOfOperatorRuns; i++ {
			storedOperatorRun, err := schedulerOperatorRunRepo.GetOperatorRun(ctx, operatorRunNames[i], serviceScheduler.OperatorTask, storedJobRun.ID)
			assert.NotNil(b, storedOperatorRun)
			assert.NoError(b, err)

			storedOperatorRuns[i] = storedOperatorRun
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			operatorRunIdx := i % maxNumberOfOperatorRuns
			operatorRun := storedOperatorRuns[operatorRunIdx]

			actualError := schedulerOperatorRunRepo.UpdateOperatorRun(ctx, serviceScheduler.OperatorTask, operatorRun.ID, operatorStartTime, serviceScheduler.StateAccepted)
			assert.NoError(b, actualError)
		}
	})
}
