// +build !unit_test

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/odpf/optimus/mock"
	testMock "github.com/stretchr/testify/mock"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestBackupRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
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
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1)
		if err != nil {
			panic(err)
		}
		m, err := NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := Migrate(dbURL); err != nil {
			panic(err)
		}

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()

		resourceSpec := models.ResourceSpec{
			ID:        uuid.Must(uuid.NewRandom()),
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

		backupUuid := uuid.Must(uuid.NewRandom())
		projectName := "project"
		destinationDataset := "optimus_backup"
		destinationTable := fmt.Sprintf("backup_playground_table_%s", backupUuid)
		//urn := fmt.Sprintf("store://%s:%s.%s", projectName, destinationDataset, destinationTable)

		backupResult := make(map[string]interface{})
		backupResult["project"] = projectName
		backupResult["dataset"] = destinationDataset
		backupResult["table"] = destinationTable

		backupSpec := models.BackupSpec{
			ID:          backupUuid,
			Resource:    resourceSpec,
			Result:      backupResult,
			Description: "description",
			Config: map[string]string{
				"ttl":     "30",
				"dataset": destinationDataset,
				"prefix":  "backup",
			},
		}

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		resourceRepo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		err := resourceRepo.Insert(ctx, resourceSpec)
		assert.Nil(t, err)

		backupRepo := NewBackupRepository(db, projectSpec, datastorer)
		err = backupRepo.Save(ctx, backupSpec)
		assert.Nil(t, err)

		backups, err := backupRepo.GetAll(ctx)
		assert.Nil(t, err)

		assert.Equal(t, backupSpec.ID, backups[0].ID)
		assert.Equal(t, backupSpec.Description, backups[0].Description)
		assert.Equal(t, backupSpec.Resource, backups[0].Resource)
		assert.Equal(t, backupSpec.Config, backups[0].Config)
		assert.Equal(t, backupSpec.Result, backups[0].Result)
	})
}
