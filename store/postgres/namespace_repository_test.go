//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestNamespaceRepository(t *testing.T) {
	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := postgres.Connect(dbURL, 1, 1, os.Stdout)
		if err != nil {
			panic(err)
		}
		m, err := postgres.NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := postgres.Migrate(dbURL); err != nil {
			panic(err)
		}

		return dbConn
	}
	ctx := context.Background()

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	secrets := []models.ProjectSecretItem{
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "g-optimus",
			Value: "secret",
		},
		{
			ID:    uuid.Must(uuid.NewRandom()),
			Name:  "t-optimus",
			Value: "super-secret",
		},
	}
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus",
		Config: map[string]string{
			"bucket":                  "gs://some_folder",
			transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
		},
	}
	namespaceSpecs := []models.NamespaceSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
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
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus",
			Config: map[string]string{
				"bucket":                  "gs://some_folder",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus-2",
			Config: map[string]string{
				"bucket":                  "gs://some_folder-2",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus-3",
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		// save project
		projRepo := postgres.NewProjectRepository(db, hash)
		err := projRepo.Save(ctx, projectSpec)
		assert.Nil(t, err)

		repo := postgres.NewNamespaceRepository(db, projectSpec, hash)
		err = repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[1])
		assert.NotNil(t, err)

		// Secrets depend on namespace
		secretRepo := postgres.NewSecretRepository(db, projectSpec, hash)
		err = secretRepo.Insert(ctx, namespaceSpecs[0], secrets[0])
		assert.Nil(t, err)
		err = secretRepo.Insert(ctx, namespaceSpecs[0], secrets[1])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
		assert.Equal(t, projectSpec.Name, checkModel.ProjectSpec.Name)
		assert.Equal(t, 2, len(checkModel.ProjectSpec.Secret))
	})

	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := namespaceSpecs[0]
			testModelB := namespaceSpecs[2]

			repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			//try for update
			err = repo.Save(ctx, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelB.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "10.12.12.12:6668,10.12.12.13:6668", checkModel.Config[transporterKafkaBrokerKey])
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := namespaceSpecs[2]

			repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

			//try for create
			testModelA.Config["bucket"] = "gs://some_folder"
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for update
			testModelA.Config["bucket"] = "gs://another_folder"
			err = repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Config["bucket"])
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := namespaceSpecs[0]
			testModelA.ID = uuid.Nil

			repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
			assert.Equal(t, 36, len(checkModel.ID.String()))
		})
		t.Run("should not update empty config", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

			err := repo.Insert(ctx, namespaceSpecs[4])
			assert.Nil(t, err)

			err = repo.Save(ctx, namespaceSpecs[4])
			assert.Equal(t, store.ErrEmptyConfig, err)

			checkModel, err := repo.GetByName(ctx, namespaceSpecs[4].Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-3", checkModel.Name)
			assert.Equal(t, 36, len(checkModel.ID.String()))
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.NamespaceSpec{}
		testModels = append(testModels, namespaceSpecs...)

		repo := postgres.NewNamespaceRepository(db, projectSpec, hash)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2])
		assert.Nil(t, err)

		checkModel, err := repo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModel))
	})
}
