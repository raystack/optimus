//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestSecretRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "sample-namespace",
		ProjectSpec: projectSpec,
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1, os.Stdout)
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

		namespaceRepo := NewNamespaceRepository(dbConn, projectSpec, hash)
		assert.Nil(t, namespaceRepo.Save(ctx, namespaceSpec))
		return dbConn
	}

	testConfigs := []models.ProjectSecretItem{
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "g-optimus",
			Value: "secret",
		},
		{
			Name: "",
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "t-optimus",
			Value: "super-secret",
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "_OPTIMUS_sample_secret",
			Value: "super-secret",
		},
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("should able to insert secret without namespace set", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModels := []models.ProjectSecretItem{}
			testModels = append(testModels, testConfigs...)

			repo := NewSecretRepository(db, projectSpec, models.NamespaceSpec{}, hash)

			err := repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, testModels[3])
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, models.SecretTypeUserDefined, checkModel.Type)

			checkModel, err = repo.GetByID(ctx, testModels[3].ID)
			assert.Nil(t, err)
			assert.Equal(t, "_OPTIMUS_sample_secret", checkModel.Name)
			assert.Equal(t, models.SecretTypeSystemDefined, checkModel.Type)
		})
		t.Run("should able to insert secret with namespace set", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModels := []models.ProjectSecretItem{}
			testModels = append(testModels, testConfigs...)

			repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

			err := repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, testModels[3])
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, models.SecretTypeUserDefined, checkModel.Type)

			checkModel, err = repo.GetByID(ctx, testModels[3].ID)
			assert.Nil(t, err)
			assert.Equal(t, "_OPTIMUS_sample_secret", checkModel.Name)
			assert.Equal(t, models.SecretTypeSystemDefined, checkModel.Type)
		})
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			//try for update
			err = repo.Save(ctx, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "super-secret", checkModel.Value)
		})
		t.Run("insert same resource twice should throw error", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

			//try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for create the same secret
			testModelA.Value = "gs://another_folder"
			err = repo.Save(ctx, testModelA)
			assert.Equal(t, "secret already exist", err.Error())
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("update same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

			//try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for update
			testModelA.Value = "gs://another_folder"
			err = repo.Update(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Value)
		})
		t.Run("update not existing secret should return error", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]

			repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

			//try for update
			err := repo.Update(ctx, testModelA)
			assert.Equal(t, fmt.Sprintf("secret %s does not exist", testModelA.Name), err.Error())
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ProjectSecretItem{}
		testModels = append(testModels, testConfigs...)

		repo := NewSecretRepository(db, projectSpec, namespaceSpec, hash)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
}
