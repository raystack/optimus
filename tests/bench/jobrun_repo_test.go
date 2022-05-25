//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

func BenchmarkJobRunRepository(b *testing.B) {
	ctx := context.Background()
	pluginRepo := inMemoryPluginRegistry()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	adapter := postgres.NewAdapter(pluginRepo)
	bq2bq := bqPlugin{}

	project := getProject(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := getNamespace(1, project)
	namespace.ID = uuid.New()

	dbSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, getSecret(i)))
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobRun := getJobRun(getJob(i, namespace, bq2bq, nil))
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, namespace, jobRun, dest)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetByScheduledAt", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		var jobIds []uuid.UUID
		for i := 0; i < 100; i++ {
			job := getJob(i, namespace, bq2bq, nil)
			job.ID = uuid.New()
			jobIds = append(jobIds, job.ID)
			jobRun := getJobRun(job)
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, namespace, jobRun, dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 100
			scheduledAt := time.Date(2021, 11, 11, 0, 0, 0, 0, time.UTC)
			jr, _, err := repo.GetByScheduledAt(ctx, jobIds[num], scheduledAt)
			if err != nil {
				panic(err)
			}
			if jr.Spec.ID != jobIds[num] {
				panic("Id does not match")
			}
		}
	})
	b.Run("GetByID", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		var jobIds []uuid.UUID
		for i := 0; i < 100; i++ {
			job := getJob(i, namespace, bq2bq, nil)
			jobRun := getJobRun(job)
			jobRun.ID = uuid.New()
			jobIds = append(jobIds, jobRun.ID)
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, namespace, jobRun, dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 100
			jr, _, err := repo.GetByID(ctx, jobIds[num])
			if err != nil {
				panic(err)
			}
			if jr.ID != jobIds[num] {
				panic("Id does not match")
			}
		}
	})
	b.Run("UpdateStatus", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		var jobIds []uuid.UUID
		for i := 0; i < 100; i++ {
			job := getJob(i, namespace, bq2bq, nil)
			jobRun := getJobRun(job)
			jobRun.ID = uuid.New()
			jobIds = append(jobIds, jobRun.ID)
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, namespace, jobRun, dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 100
			err := repo.UpdateStatus(ctx, jobIds[num], models.RunStateSuccess)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetByTrigger", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		for i := 0; i < 100; i++ {
			job := getJob(i, namespace, bq2bq, nil)
			jobRun := getJobRun(job)
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, namespace, jobRun, dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jrs, err := repo.GetByTrigger(ctx, models.TriggerManual, models.RunStateRunning)
			if err != nil {
				panic(err)
			}
			if len(jrs) != 100 {
				panic("Did not fetch all the runs")
			}
		}
	})
	b.Run("AddInstance", func(b *testing.B) {
		db := dbSetup()

		var repo store.JobRunRepository = postgres.NewJobRunRepository(db, adapter)
		job := getJob(1, namespace, bq2bq, nil)
		jobRun := getJobRun(job)
		jobRun.ID = uuid.New()
		dest := fmt.Sprintf("bigquery://integration:playground.table%d", 1)
		err := repo.Save(ctx, namespace, jobRun, dest)
		assert.Nil(b, err)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := repo.AddInstance(ctx, namespace, jobRun, getInstanceSpecs()[0])
			if err != nil {
				panic(err)
			}
		}
	})
}

func getInstanceSpecs() []models.InstanceSpec {
	return []models.InstanceSpec{
		{
			ID:         uuid.New(),
			Name:       "do-this",
			Type:       models.InstanceTypeTask,
			ExecutedAt: time.Date(2020, 11, 11, 1, 0, 0, 0, time.UTC),
			Status:     models.RunStateAccepted,
			Data: []models.InstanceSpecData{
				{Name: "dstart", Value: "2020-01-02", Type: models.InstanceDataTypeEnv},
			},
		},
		{
			ID:   uuid.New(),
			Name: "do-that",
			Type: models.InstanceTypeTask,
		},
	}
}

func getJobRun(job models.JobSpec) models.JobRun {
	return models.JobRun{
		Spec:        job,
		ExecutedAt:  time.Time{},
		Trigger:     models.TriggerManual,
		Status:      models.RunStateRunning,
		Instances:   getInstanceSpecs(),
		ScheduledAt: time.Date(2021, 11, 11, 0, 0, 0, 0, time.UTC),
	}
}
