//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
)

func TestIntegrationSecretRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "sample-namespace",
		ProjectSpec: projectSpec,
	}

	otherNamespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "other-namespace",
		ProjectSpec: projectSpec,
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))

		namespaceRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(t, namespaceRepo.Save(ctx, projectSpec, namespaceSpec))
		assert.Nil(t, namespaceRepo.Save(ctx, projectSpec, otherNamespaceSpec))
		return dbConn
	}

	testConfigs := []models.ProjectSecretItem{
		{
			ID:    uuid.New(),
			Name:  "g-optimus",
			Value: "secret",
			Type:  models.SecretTypeUserDefined,
		},
		{
			Name: "",
		},
		{
			ID:    uuid.New(),
			Name:  "t-optimus",
			Value: "super-secret",
			Type:  models.SecretTypeUserDefined,
		},
		{
			ID:    uuid.New(),
			Name:  "_OPTIMUS_sample_secret",
			Value: "super-secret",
			Type:  models.SecretTypeSystemDefined,
		},
		{
			ID:    uuid.New(),
			Name:  "t-optimus-delete",
			Value: "super-secret",
			Type:  models.SecretTypeUserDefined,
		},
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("should able to insert secret without namespace set", func(t *testing.T) {
			db := DBSetup()
			testModels := []models.ProjectSecretItem{}
			testModels = append(testModels, testConfigs...)

			repo := postgres.NewSecretRepository(db, hash)

			err := repo.Insert(ctx, projectSpec, models.NamespaceSpec{}, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, projectSpec, models.NamespaceSpec{}, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, projectSpec, models.NamespaceSpec{}, testModels[3])
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModels[0].Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, models.SecretTypeUserDefined, checkModel.Type)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModels[3].Name)
			assert.Nil(t, err)
			assert.Equal(t, "_OPTIMUS_sample_secret", checkModel.Name)
			assert.Equal(t, models.SecretTypeSystemDefined, checkModel.Type)
		})
		t.Run("should able to insert secret with namespace set", func(t *testing.T) {
			db := DBSetup()
			testModels := []models.ProjectSecretItem{}
			testModels = append(testModels, testConfigs...)

			repo := postgres.NewSecretRepository(db, hash)

			err := repo.Insert(ctx, projectSpec, namespaceSpec, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, projectSpec, namespaceSpec, testModels[1])
			assert.NotNil(t, err)

			err = repo.Insert(ctx, projectSpec, namespaceSpec, testModels[3])
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModels[0].Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, models.SecretTypeUserDefined, checkModel.Type)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModels[3].Name)
			assert.Nil(t, err)
			assert.Equal(t, "_OPTIMUS_sample_secret", checkModel.Name)
			assert.Equal(t, models.SecretTypeSystemDefined, checkModel.Type)
		})
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := postgres.NewSecretRepository(db, hash)

			// try for create
			err := repo.Save(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			// try for update
			err = repo.Save(ctx, projectSpec, namespaceSpec, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModelB.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "super-secret", checkModel.Value)
		})
		t.Run("insert same resource twice should throw error", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[2]

			repo := postgres.NewSecretRepository(db, hash)

			// try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			// try for create the same secret
			testModelA.Value = "gs://another_folder"
			err = repo.Save(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Equal(t, "resource already exists", err.Error())
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("update same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[2]

			repo := postgres.NewSecretRepository(db, hash)

			// try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			// try for update
			testModelA.Value = "gs://another_folder"
			err = repo.Update(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Value)
		})
		t.Run("update not existing secret should return error", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]

			repo := postgres.NewSecretRepository(db, hash)

			// try for update
			err := repo.Update(ctx, projectSpec, namespaceSpec, testModelA)
			assert.Equal(t, "resource not found", err.Error())
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.ProjectSecretItem{}
		testModels = append(testModels, testConfigs...)

		repo := postgres.NewSecretRepository(db, hash)

		err := repo.Insert(ctx, projectSpec, namespaceSpec, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, projectSpec, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
	t.Run("GetAllWithUpstreams", func(t *testing.T) {
		t.Run("should get all the secrets for a project", func(t *testing.T) {
			db := DBSetup()

			var otherModels []models.ProjectSecretItem
			otherModels = append(otherModels, testConfigs...)

			repo := postgres.NewSecretRepository(db, hash)
			assert.Nil(t, repo.Insert(ctx, projectSpec, otherNamespaceSpec, otherModels[0]))
			assert.Nil(t, repo.Insert(ctx, projectSpec, otherNamespaceSpec, otherModels[3]))

			var testModels []models.ProjectSecretItem
			testModels = append(testModels, testConfigs...)
			assert.Nil(t, repo.Insert(ctx, projectSpec, namespaceSpec, testModels[2]))
			assert.Nil(t, repo.Insert(ctx, projectSpec, namespaceSpec, testModels[4]))
			err := repo.Delete(ctx, projectSpec, namespaceSpec, testModels[4].Name)
			assert.Nil(t, err)

			allSecrets, err := repo.GetAll(ctx, projectSpec)
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
	t.Run("GetSecrets", func(t *testing.T) {
		t.Run("should get all the secrets for a namespace", func(t *testing.T) {
			db := DBSetup()

			repo := postgres.NewSecretRepository(db, hash)

			var otherModels []models.ProjectSecretItem
			otherModels = append(otherModels, testConfigs...)
			// Other namespace
			assert.Nil(t, repo.Insert(ctx, projectSpec, otherNamespaceSpec, otherModels[0]))
			// No namespace
			assert.Nil(t, repo.Insert(ctx, projectSpec, models.NamespaceSpec{}, otherModels[4]))

			var testModels []models.ProjectSecretItem
			testModels = append(testModels, testConfigs...)
			assert.Nil(t, repo.Insert(ctx, projectSpec, namespaceSpec, testModels[2]))
			// System defined secret
			assert.Nil(t, repo.Insert(ctx, projectSpec, namespaceSpec, testModels[3]))

			allSecrets, err := repo.GetSecrets(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)
			assert.Equal(t, len(allSecrets), 2)

			assert.Equal(t, allSecrets[0].ID, otherModels[4].ID)
			assert.Equal(t, allSecrets[0].Name, otherModels[4].Name)
			assert.Equal(t, allSecrets[0].Value, otherModels[4].Value)
			assert.Equal(t, allSecrets[0].Type, models.SecretTypeUserDefined)

			assert.Equal(t, allSecrets[1].ID, testModels[2].ID)
			assert.Equal(t, allSecrets[1].Name, testModels[2].Name)
			assert.Equal(t, allSecrets[1].Value, testModels[2].Value)
			assert.Equal(t, allSecrets[1].Type, models.SecretTypeUserDefined)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("deletes the secret for namespace", func(t *testing.T) {
			db := DBSetup()

			secret := models.ProjectSecretItem{
				ID:    uuid.New(),
				Name:  "t-optimus-delete",
				Value: "super-secret",
				Type:  models.SecretTypeUserDefined,
			}
			repo := postgres.NewSecretRepository(db, hash)

			assert.Nil(t, repo.Insert(ctx, projectSpec, namespaceSpec, secret))
			_, err := repo.GetByName(ctx, projectSpec, secret.Name)
			assert.Nil(t, err)

			err = repo.Delete(ctx, projectSpec, namespaceSpec, secret.Name)
			assert.Nil(t, err)

			_, err = repo.GetByName(ctx, projectSpec, secret.Name)
			assert.NotNil(t, err)
			assert.Equal(t, store.ErrResourceNotFound, err)
		})
		t.Run("deletes the secret for project", func(t *testing.T) {
			db := DBSetup()

			secret := models.ProjectSecretItem{
				ID:    uuid.New(),
				Name:  "t-optimus-delete",
				Value: "super-secret",
				Type:  models.SecretTypeUserDefined,
			}
			repo := postgres.NewSecretRepository(db, hash)

			assert.Nil(t, repo.Insert(ctx, projectSpec, models.NamespaceSpec{}, secret))
			_, err := repo.GetByName(ctx, projectSpec, secret.Name)
			assert.Nil(t, err)

			err = repo.Delete(ctx, projectSpec, models.NamespaceSpec{}, secret.Name)
			assert.Nil(t, err)

			_, err = repo.GetByName(ctx, projectSpec, secret.Name)
			assert.NotNil(t, err)
			assert.Equal(t, store.ErrResourceNotFound, err)
		})
		t.Run("returns error when non existing is deleted", func(t *testing.T) {
			db := DBSetup()

			repo := postgres.NewSecretRepository(db, hash)

			err := repo.Delete(ctx, projectSpec, namespaceSpec, "invalid")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found", err.Error())
		})
	})
}
