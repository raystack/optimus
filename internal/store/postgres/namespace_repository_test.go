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

func TestIntegrationNamespaceRepository(t *testing.T) {
	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		return dbConn
	}
	ctx := context.Background()

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	secrets := []models.ProjectSecretItem{
		{
			ID:    uuid.New(),
			Name:  "g-optimus",
			Value: "secret",
		},
		{
			ID:    uuid.New(),
			Name:  "t-optimus",
			Value: "super-secret",
		},
	}
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus",
		Config: map[string]string{
			"bucket":                  "gs://some_folder",
			transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
		},
	}
	namespaceSpecs := []models.NamespaceSpec{
		{
			ID:   uuid.New(),
			Name: "g-optimus",
			Config: map[string]string{
				"bucket":                  "gs://some_folder",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			Name: "",
		},
		{
			ID:   uuid.New(),
			Name: "t-optimus",
			Config: map[string]string{
				"bucket":                  "gs://some_folder",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   uuid.New(),
			Name: "t-optimus-2",
			Config: map[string]string{
				"bucket":                  "gs://some_folder-2",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   uuid.New(),
			Name: "t-optimus-3",
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		// save project
		projRepo := postgres.NewProjectRepository(db, hash)
		err := projRepo.Save(ctx, projectSpec)
		assert.Nil(t, err)

		repo := postgres.NewNamespaceRepository(db, hash)
		err = repo.Insert(ctx, projectSpec, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, projectSpec, testModels[1])
		assert.NotNil(t, err)

		// Secrets depend on namespace
		secretRepo := postgres.NewSecretRepository(db, hash)
		err = secretRepo.Insert(ctx, projectSpec, namespaceSpecs[0], secrets[0])
		assert.Nil(t, err)
		err = secretRepo.Insert(ctx, projectSpec, namespaceSpecs[0], secrets[1])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, projectSpec, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
		assert.Equal(t, projectSpec.Name, checkModel.ProjectSpec.Name)
		assert.Equal(t, 2, len(checkModel.ProjectSpec.Secret))
	})

	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			testModelA := namespaceSpecs[0]
			testModelB := namespaceSpecs[2]

			repo := postgres.NewNamespaceRepository(db, hash)

			// try for create
			err := repo.Save(ctx, projectSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			// try for update
			err = repo.Save(ctx, projectSpec, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModelB.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "10.12.12.12:6668,10.12.12.13:6668", checkModel.Config[transporterKafkaBrokerKey])
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			testModelA := namespaceSpecs[2]

			repo := postgres.NewNamespaceRepository(db, hash)

			// try for create
			testModelA.Config["bucket"] = "gs://some_folder"
			err := repo.Save(ctx, projectSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			// try for update
			testModelA.Config["bucket"] = "gs://another_folder"
			err = repo.Save(ctx, projectSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Config["bucket"])
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			testModelA := namespaceSpecs[0]
			testModelA.ID = uuid.Nil

			repo := postgres.NewNamespaceRepository(db, hash)

			// try for create
			err := repo.Save(ctx, projectSpec, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, 36, len(checkModel.ID.String()))
		})
		t.Run("should not update empty config", func(t *testing.T) {
			db := DBSetup()

			repo := postgres.NewNamespaceRepository(db, hash)

			err := repo.Insert(ctx, projectSpec, namespaceSpecs[4])
			assert.Nil(t, err)

			err = repo.Save(ctx, projectSpec, namespaceSpecs[4])
			assert.Equal(t, store.ErrEmptyConfig, err)

			checkModel, err := repo.GetByName(ctx, projectSpec, namespaceSpecs[4].Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-3", checkModel.Name)
			assert.Equal(t, 36, len(checkModel.ID.String()))
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		repo := postgres.NewNamespaceRepository(db, hash)

		err := repo.Insert(ctx, projectSpec, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, projectSpec, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})

	t.Run("Get", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		repo := postgres.NewNamespaceRepository(db, hash)
		err := repo.Insert(ctx, projectSpec, testModels[0])
		assert.Nil(t, err)

		secretRepo := postgres.NewSecretRepository(db, hash)
		err = secretRepo.Insert(ctx, projectSpec, testModels[0], secrets[0])
		assert.Nil(t, err)

		namespace, err := repo.Get(ctx, projectSpec.Name, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", namespace.Name)
		assert.Equal(t, "t-optimus", namespace.ProjectSpec.Name)
		assert.Equal(t, "g-optimus", namespace.ProjectSpec.Secret[0].Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		repo := postgres.NewNamespaceRepository(db, hash)

		err := repo.Insert(ctx, projectSpec, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, projectSpec, testModels[2])
		assert.Nil(t, err)

		checkModel, err := repo.GetAll(ctx, projectSpec)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModel))
	})
}
