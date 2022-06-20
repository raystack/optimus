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

func getJob(i int, namespace models.NamespaceSpec, bq2bq models.DependencyResolverMod, hookUnit models.BasePlugin) models.JobSpec { //nolint:unparam
	jobConfig := []models.JobSpecConfigItem{
		{Name: "DATASET", Value: "playground"},
		{Name: "JOB_LABELS", Value: "owner=optimus"},
		{Name: "LOAD_METHOD", Value: "REPLACE"},
		{Name: "PROJECT", Value: "integration"},
		{Name: "SQL_TYPE", Value: "STANDARD"},
		{Name: "TABLE", Value: fmt.Sprintf("table%d", i)},
		{Name: "TASK_TIMEZONE", Value: "UTC"},
		{Name: "SECRET_NAME", Value: "{{.secret.secret3}}"},
		{Name: "TASK_BQ2BQ", Value: "{{.secret.TASK_BQ2BQ}}"},
	}

	jobMeta := models.JobSpecMetadata{
		Resource: models.JobSpecResource{
			Request: models.JobSpecResourceConfig{CPU: "200m", Memory: "1g"},
			Limit:   models.JobSpecResourceConfig{CPU: "1000m", Memory: "2g"},
		},
	}

	window := models.JobSpecTaskWindow{
		Size:       time.Hour * 24,
		Offset:     time.Second * 0,
		TruncateTo: "h",
	}
	var hooks []models.JobSpecHook
	if hookUnit != nil {
		hooks = append(hooks, models.JobSpecHook{
			Config: []models.JobSpecConfigItem{
				{
					Name:  "FILTER_EXPRESSION",
					Value: "event_timestamp > 10000",
				},
			},
			Unit: &models.Plugin{Base: hookUnit},
		})
	}

	jobSpec := models.JobSpec{
		Version:     1,
		Name:        fmt.Sprintf("job_%d", i),
		Description: "A test job for benchmarking deploy",
		Labels:      map[string]string{"orchestrator": "optimus"},
		Owner:       "Benchmark",
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC),
			EndDate:   nil,
			Interval:  "0 8 * * *",
		},
		Behavior: models.JobSpecBehavior{
			DependsOnPast: false,
			CatchUp:       false,
			Retry: models.JobSpecBehaviorRetry{
				Count:              2,
				Delay:              time.Millisecond * 100,
				ExponentialBackoff: true,
			},
			Notify: nil,
		},
		Task: models.JobSpecTask{
			Unit: &models.Plugin{
				Base:          bq2bq,
				DependencyMod: bq2bq,
			},
			Priority: 2000,
			Window:   window,
			Config:   jobConfig,
		},
		Dependencies: nil,
		Assets: *models.JobAssets{}.New(
			[]models.JobSpecAsset{
				{
					Name: "query.sql",
					Value: `WITH Characters AS
 (SELECT '{{.secret.secret3}}' as name, 51 as age, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Uchiha', 77, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Saitama', 77, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Sanchez', 52, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp)
SELECT * FROM Characters`,
				},
			},
		),
		Hooks:    hooks,
		Metadata: jobMeta,
		ExternalDependencies: models.ExternalDependency{
			HTTPDependencies: []models.HTTPDependency{
				{
					Name: "test_http_sensor_1",
					RequestParams: map[string]string{
						"key_test": "value_test",
					},
					URL: "http://test/optimus/status/1",
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				},
			},
		},
		NamespaceSpec: namespace,
	}

	return jobSpec
}
