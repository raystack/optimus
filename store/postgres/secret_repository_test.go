//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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

	otherNamespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "other-namespace",
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
		assert.Nil(t, namespaceRepo.Save(ctx, otherNamespaceSpec))
		return dbConn
	}

	testConfigs := []models.ProjectSecretItem{
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "g-optimus",
			Value: "secret",
			Type:  models.SecretTypeUserDefined,
		},
		{
			Name: "",
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "t-optimus",
			Value: "super-secret",
			Type:  models.SecretTypeUserDefined,
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "_OPTIMUS_sample_secret",
			Value: "super-secret",
			Type:  models.SecretTypeSystemDefined,
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "t-optimus-delete",
			Value: "super-secret",
			Type:  models.SecretTypeUserDefined,
		},
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("should able to insert secret without namespace set", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModels := []models.ProjectSecretItem{}
			testModels = append(testModels, testConfigs...)

			repo := NewSecretRepository(db, projectSpec, hash)

			err := repo.Insert(ctx, models.NamespaceSpec{}, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, models.NamespaceSpec{}, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, models.NamespaceSpec{}, testModels[3])
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

			repo := NewSecretRepository(db, projectSpec, hash)

			err := repo.Insert(ctx, namespaceSpec, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, namespaceSpec, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, namespaceSpec, testModels[3])
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

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			err := repo.Save(ctx, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			//try for update
			err = repo.Save(ctx, namespaceSpec, testModelB)
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

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for create the same secret
			testModelA.Value = "gs://another_folder"
			err = repo.Save(ctx, namespaceSpec, testModelA)
			assert.Equal(t, "resource already exists", err.Error())
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("update same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for update
			testModelA.Value = "gs://another_folder"
			err = repo.Update(ctx, namespaceSpec, testModelA)
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

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for update
			err := repo.Update(ctx, namespaceSpec, testModelA)
			assert.Equal(t, "resource not found", err.Error())
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ProjectSecretItem{}
		testModels = append(testModels, testConfigs...)

		repo := NewSecretRepository(db, projectSpec, hash)

		err := repo.Insert(ctx, namespaceSpec, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("should get all the secrets for a project", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			var otherModels []models.ProjectSecretItem
			otherModels = append(otherModels, testConfigs...)
			repo := NewSecretRepository(db, projectSpec, hash)
			assert.Nil(t, repo.Insert(ctx, otherNamespaceSpec, otherModels[0]))
			assert.Nil(t, repo.Insert(ctx, otherNamespaceSpec, otherModels[3]))

			var testModels []models.ProjectSecretItem
			testModels = append(testModels, testConfigs...)
			assert.Nil(t, repo.Insert(ctx, namespaceSpec, testModels[2]))
			assert.Nil(t, repo.Insert(ctx, namespaceSpec, testModels[4]))
			repo.db.Table("secret").Delete(&testModels[4])

			allSecrets, err := repo.GetAll(ctx)
			assert.Nil(t, err)
			assert.Len(t, allSecrets, 2)

			assert.Equal(t, allSecrets[0].ID, otherModels[0].ID)
			assert.Equal(t, allSecrets[0].Name, otherModels[0].Name)
			assert.Equal(t, allSecrets[0].Namespace, otherNamespaceSpec.Name)
			assert.Equal(t, string(allSecrets[0].Type), string(otherModels[0].Type))

			assert.Equal(t, allSecrets[1].ID, testModels[2].ID)
			assert.Equal(t, allSecrets[1].Name, testModels[2].Name)
			assert.Equal(t, allSecrets[1].Namespace, namespaceSpec.Name)
			assert.Equal(t, string(allSecrets[1].Type), string(testModels[2].Type))
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("deletes the secret for namespace", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			secret := models.ProjectSecretItem{
				ID:    uuid.Must(uuid.NewRandom()),
				Name:  "t-optimus-delete",
				Value: "super-secret",
				Type:  models.SecretTypeUserDefined,
			}
			repo := NewSecretRepository(db, projectSpec, hash)

			assert.Nil(t, repo.Insert(ctx, namespaceSpec, secret))
			_, err := repo.GetByName(ctx, secret.Name)
			assert.Nil(t, err)

			err = repo.Delete(ctx, namespaceSpec, secret.Name)
			assert.Nil(t, err)

			_, err = repo.GetByName(ctx, secret.Name)
			assert.NotNil(t, err)
			assert.Equal(t, store.ErrResourceNotFound, err)
		})
		t.Run("deletes the secret for project", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			secret := models.ProjectSecretItem{
				ID:    uuid.Must(uuid.NewRandom()),
				Name:  "t-optimus-delete",
				Value: "super-secret",
				Type:  models.SecretTypeUserDefined,
			}
			repo := NewSecretRepository(db, projectSpec, hash)

			assert.Nil(t, repo.Insert(ctx, models.NamespaceSpec{}, secret))
			_, err := repo.GetByName(ctx, secret.Name)
			assert.Nil(t, err)

			err = repo.Delete(ctx, models.NamespaceSpec{}, secret.Name)
			assert.Nil(t, err)

			_, err = repo.GetByName(ctx, secret.Name)
			assert.NotNil(t, err)
			assert.Equal(t, store.ErrResourceNotFound, err)
		})
		t.Run("returns error when non existing is deleted", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			repo := NewSecretRepository(db, projectSpec, hash)

			err := repo.Delete(ctx, namespaceSpec, "invalid")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found", err.Error())
		})
	})
}
