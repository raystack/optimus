//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	testMock "github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
)

func TestIntegrationBackupRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	ctx := context.Background()

	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)
	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)
	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable: dsTypeTableController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)
	datastorer.On("Name").Return("DS")

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		resourceSpec := models.ResourceSpec{
			ID:        uuid.New(),
			Version:   1,
			Name:      "proj.datas.test",
			Datastore: datastorer,
			Type:      models.ResourceTypeTable,
			Spec:      nil,
			URN:       "datastore://proj:datas.test",
		}
		dsTypeTableAdapter.On("ToYaml", resourceSpec).Return([]byte("some binary data"), nil)
		dsTypeTableAdapter.On("FromYaml", []byte("some binary data")).Return(resourceSpec, nil)

		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(resourceSpec.URN, nil).Twice()

		backupUUID := uuid.New()
		projectName := "project"
		destinationDataset := "optimus_backup"
		destinationTable := fmt.Sprintf("backup_playground_table_%s", backupUUID)

		backupResult := make(map[string]interface{})
		backupResult["project"] = projectName
		backupResult["dataset"] = destinationDataset
		backupResult["table"] = destinationTable

		backupSpec := models.BackupSpec{
			ID:          backupUUID,
			Resource:    resourceSpec,
			Result:      backupResult,
			Description: "description",
			Config: map[string]string{
				"ttl":     "30",
				"dataset": destinationDataset,
				"prefix":  "backup",
			},
		}

		projectResourceSpecRepo := postgres.NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		resourceRepo := postgres.NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		err := resourceRepo.Insert(ctx, resourceSpec)
		assert.Nil(t, err)

		backupRepo := postgres.NewBackupRepository(db)
		err = backupRepo.Save(ctx, backupSpec)
		assert.Nil(t, err)

		backups, err := backupRepo.GetAll(ctx, projectSpec, datastorer)
		assert.Nil(t, err)

		assert.Equal(t, backupSpec.ID, backups[0].ID)
		assert.Equal(t, backupSpec.Description, backups[0].Description)
		assert.Equal(t, backupSpec.Resource, backups[0].Resource)
		assert.Equal(t, backupSpec.Config, backups[0].Config)
		assert.Equal(t, backupSpec.Result, backups[0].Result)

		backup, err := backupRepo.GetByID(ctx, backupUUID, datastorer)
		assert.Nil(t, err)

		assert.Equal(t, backupSpec.ID, backup.ID)
		assert.Equal(t, backupSpec.Description, backup.Description)
		assert.Equal(t, backupSpec.Resource, backup.Resource)
		assert.Equal(t, backupSpec.Config, backup.Config)
		assert.Equal(t, backupSpec.Result, backup.Result)
	})
}
