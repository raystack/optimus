//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

func BenchmarkReplayRepository(b *testing.B) {
	ctx := context.Background()
	pluginRepo := inMemoryPluginRegistry()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	adapter := postgres.NewAdapter(pluginRepo)
	bq2bq := bqPlugin{}

	project := getProject(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := getNamespace(1, project)
	namespace.ID = uuid.New()

	var jobs []models.JobSpec
	for i := 0; i < 20; i++ {
		job := getJob(i, namespace, bq2bq, nil)
		job.ID = uuid.New()
		jobs = append(jobs, job)
	}

	dbSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, project, hash)
		assert.Nil(b, nsRepo.Save(ctx, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, getSecret(i)))
		}

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(dbConn, project, adapter)
		jobRepo := postgres.NewJobSpecRepository(dbConn, namespace, projectJobSpecRepo, adapter)

		for i := 0; i < len(jobs); i++ {
			err := jobRepo.Insert(ctx, jobs[i])
			assert.Nil(b, err)
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()

		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			err := repo.Insert(ctx, replay)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetByID", func(b *testing.B) {
		db := dbSetup()
		var replayIds []uuid.UUID
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			replayIds = append(replayIds, replay.ID)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 100
			replaySpec, err := repo.GetByID(ctx, replayIds[num])
			if err != nil {
				panic(err)
			}
			if replaySpec.ID != replayIds[num] {
				panic("Replay id is not same")
			}
		}
	})
	b.Run("GetByStatus", func(b *testing.B) {
		db := dbSetup()
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			specs, err := repo.GetByStatus(ctx, []string{models.ReplayStatusAccepted})
			if err != nil {
				panic(err)
			}
			if len(specs) == 0 {
				panic("Should return specs")
			}
		}
	})
	b.Run("GetByJobIDAndStatus", func(b *testing.B) {
		db := dbSetup()
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 20
			specs, err := repo.GetByJobIDAndStatus(ctx, jobs[num].ID, []string{models.ReplayStatusAccepted})
			if err != nil {
				panic(err)
			}
			if len(specs) == 0 {
				panic("Should return specs")
			}
		}
	})
	b.Run("GetByProjectIDAndStatus", func(b *testing.B) {
		db := dbSetup()
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			specs, err := repo.GetByProjectIDAndStatus(ctx, project.ID, []string{models.ReplayStatusAccepted})
			if err != nil {
				panic(err)
			}
			if len(specs) == 0 {
				panic("Should return specs")
			}
		}
	})
	b.Run("GetByProjectID", func(b *testing.B) {
		db := dbSetup()
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			specs, err := repo.GetByProjectID(ctx, project.ID)
			if err != nil {
				panic(err)
			}
			if len(specs) == 0 {
				panic("Should return specs")
			}
		}
	})
	b.Run("UpdateStatus", func(b *testing.B) {
		db := dbSetup()
		var replayIds []uuid.UUID
		var repo store.ReplaySpecRepository = postgres.NewReplayRepository(db, adapter)
		for i := 0; i < 100; i++ {
			num := i % 20
			replay := getReplaySpec(jobs[num], 4)
			replayIds = append(replayIds, replay.ID)
			err := repo.Insert(ctx, replay)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 100
			err := repo.UpdateStatus(ctx, replayIds[num], models.ReplayStatusSuccess, models.ReplayMessage{
				Type:    models.ReplayStatusSuccess,
				Message: job.ReplayMessageSuccess,
			})
			if err != nil {
				panic(err)
			}
		}
	})
}

func getReplaySpec(job models.JobSpec, numOfRuns int) *models.ReplaySpec { //nolint:unparam
	startTime := time.Date(2021, 1, 15, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2021, 1, 15+numOfRuns, 0, 0, 0, 0, time.UTC)
	treeNode := tree.NewTreeNode(job)

	for k := 0; k < numOfRuns; k++ {
		run := time.Date(2021, 1, 15+k, 2, 0, 0, 0, time.UTC)
		treeNode.Runs.Add(run)
	}

	return &models.ReplaySpec{
		ID:            uuid.New(),
		Job:           job,
		StartDate:     startTime,
		EndDate:       endTime,
		Config:        map[string]string{models.ConfigIgnoreDownstream: "true"},
		ExecutionTree: treeNode,
		Status:        models.ReplayStatusAccepted,
	}
}
