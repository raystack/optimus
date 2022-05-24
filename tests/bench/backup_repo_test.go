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

func BenchmarkDatastoreBackupRepository(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	bigqueryStore := getBigQueryDataStore()

	project := getProject(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := getNamespace(1, project)
	namespace.ID = uuid.New()

	resource := getResourceSpec(1, bigqueryStore)
	resource.ID = uuid.New()

	DBSetup := func() *gorm.DB {
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

		projectResourceSpecRepo := postgres.NewProjectResourceSpecRepository(dbConn, project, bigqueryStore)
		resourceRepo := postgres.NewResourceSpecRepository(dbConn, namespace, bigqueryStore, projectResourceSpecRepo)

		err := resourceRepo.Insert(ctx, resource)
		assert.Nil(b, err)

		return dbConn
	}
	b.Run("Save", func(b *testing.B) {
		db := DBSetup()

		var repo store.BackupRepository = postgres.NewBackupRepository(db)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			backup := getBackupSpec(i, resource)
			err := repo.Save(ctx, backup)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetAll", func(b *testing.B) {
		db := DBSetup()

		var repo store.BackupRepository = postgres.NewBackupRepository(db)
		for i := 0; i < 100; i++ {
			backup := getBackupSpec(i, resource)
			err := repo.Save(ctx, backup)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			bkp, err := repo.GetAll(ctx, project, bigqueryStore)
			if err != nil {
				panic(err)
			}
			if len(bkp) != 100 {
				panic("Not all result returned")
			}
		}
	})
	b.Run("GetByID", func(b *testing.B) {
		db := DBSetup()

		var repo store.BackupRepository = postgres.NewBackupRepository(db)
		var ids []uuid.UUID
		for i := 0; i < 10; i++ {
			backup := getBackupSpec(i, resource)
			id := uuid.New()
			ids = append(ids, id)
			backup.ID = id
			err := repo.Save(ctx, backup)
			if err != nil {
				panic(err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 10
			id := ids[num]
			bk, err := repo.GetByID(ctx, id, bigqueryStore)
			if err != nil {
				panic(err)
			}
			if bk.ID != id {
				panic("Id should be same")
			}
		}
	})
}

func getBackupSpec(i int, resourceSpec models.ResourceSpec) models.BackupSpec {
	projectName := "project"
	destinationDataset := "optimus_backup"
	destinationTable := fmt.Sprintf("backup_playground_table_%d", i)

	backupResult := map[string]interface{}{
		"project": projectName,
		"dataset": destinationDataset,
		"table":   destinationTable,
	}

	return models.BackupSpec{
		ID:          uuid.New(),
		Resource:    resourceSpec,
		Result:      backupResult,
		Description: "description",
		Config: map[string]string{
			"ttl":     "30",
			"dataset": destinationDataset,
			"prefix":  "backup",
		},
		CreatedAt: time.Date(2022, 3, 10, 5, 5, 5, 0, time.UTC),
	}
}
