//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobRepository(b *testing.B) {
	ctx := context.Background()
	pluginRepo := setup.InMemoryPluginRegistry()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	adapter := postgres.NewAdapter(pluginRepo)

	project := setup.Project(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, project)
	namespace.ID = uuid.New()

	bq2bq := setup.MockPluginBQ{}

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, setup.Secret(i)))
		}

		return dbConn
	}

	b.Run("Save", func(t *testing.B) {
		db := dbSetup()

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var repo store.NamespaceJobSpecRepository = postgres.NewNamespaceJobSpecRepository(db, namespace, projectJobSpecRepo, *jobRunMetricsRepository, adapter)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			jobSpec := setup.Job(i, namespace, bq2bq, nil)
			err := repo.Save(ctx, jobSpec, dest)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByName", func(t *testing.B) {
		db := dbSetup()

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var repo store.NamespaceJobSpecRepository = postgres.NewNamespaceJobSpecRepository(db, namespace, projectJobSpecRepo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("job_%d", num)

			jb, err := repo.GetByName(ctx, name)
			if err != nil {
				panic(err)
			}
			if jb.Name != name {
				panic("Job name does not match")
			}
		}
	})

	b.Run("GetAll", func(t *testing.B) {
		db := dbSetup()

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var repo store.NamespaceJobSpecRepository = postgres.NewNamespaceJobSpecRepository(db, namespace, projectJobSpecRepo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := repo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jbs, err := repo.GetAll(ctx)
			if err != nil {
				panic(err)
			}
			if len(jbs) != 1000 {
				panic("Job length does not match")
			}
		}
	})
}

func BenchmarkProjectJobRepo(b *testing.B) {
	ctx := context.Background()
	pluginRepo := setup.InMemoryPluginRegistry()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	adapter := postgres.NewAdapter(pluginRepo)

	project := setup.Project(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, project)
	namespace.ID = uuid.New()

	bq2bq := setup.MockPluginBQ{}

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, setup.Secret(i)))
		}

		return dbConn
	}

	b.Run("GetByName", func(t *testing.B) {
		db := dbSetup()

		var repo store.ProjectJobSpecRepository = postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var namespaceJobSpecRepo = postgres.NewNamespaceJobSpecRepository(db, namespace, repo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := namespaceJobSpecRepo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("job_%d", num)

			jb, _, err := repo.GetByName(ctx, name)
			if err != nil {
				panic(err)
			}
			if jb.Name != name {
				panic("Job name does not match")
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()

		var repo store.ProjectJobSpecRepository = postgres.NewProjectJobSpecRepository(db, project, adapter)

		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)

		var namespaceJobSpecRepo = postgres.NewNamespaceJobSpecRepository(db, namespace, repo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := namespaceJobSpecRepo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jbs, err := repo.GetAll(ctx)
			if err != nil {
				panic(err)
			}
			if len(jbs) != 1000 {
				panic("Job length does not match")
			}
		}
	})

	b.Run("GetByNameForProject", func(b *testing.B) {
		db := dbSetup()

		var repo store.ProjectJobSpecRepository = postgres.NewProjectJobSpecRepository(db, project, adapter)

		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var namespaceJobSpecRepo = postgres.NewNamespaceJobSpecRepository(db, namespace, repo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := namespaceJobSpecRepo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("job_%d", num)

			jb, _, err := repo.GetByNameForProject(ctx, project.Name, name)
			if err != nil {
				panic(err)
			}
			if jb.Name != name {
				panic("Job name is not same")
			}
		}
	})

	b.Run("GetJobNamespaces", func(b *testing.B) {
		db := dbSetup()

		var repo store.ProjectJobSpecRepository = postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var namespaceJobSpecRepo = postgres.NewNamespaceJobSpecRepository(db, namespace, repo, *jobRunMetricsRepository, adapter)

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := namespaceJobSpecRepo.Save(ctx, setup.Job(i, namespace, bq2bq, nil), dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := repo.GetJobNamespaces(ctx)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByIDs", func(b *testing.B) {
		db := dbSetup()

		var repo store.ProjectJobSpecRepository = postgres.NewProjectJobSpecRepository(db, project, adapter)
		taskRunRepository := postgres.NewTaskRunRepository(db)
		sensorRunRepository := postgres.NewSensorRunRepository(db)
		hookRunRepository := postgres.NewHookRunRepository(db)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)
		var namespaceJobSpecRepo = postgres.NewNamespaceJobSpecRepository(db, namespace, repo, *jobRunMetricsRepository, adapter)

		var ids []uuid.UUID
		for i := 0; i < 1000; i++ {
			id := uuid.New()
			ids = append(ids, id)
			spec := setup.Job(i, namespace, bq2bq, nil)
			spec.ID = id
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			err := namespaceJobSpecRepo.Save(ctx, spec, dest)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			jbs, err := repo.GetByIDs(ctx, []uuid.UUID{ids[num]})
			if err != nil {
				panic(err)
			}
			if len(jbs) != 1 {
				panic("Length of jobs is 1")
			}
		}
	})
}
