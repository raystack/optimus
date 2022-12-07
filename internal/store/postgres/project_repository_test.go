//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
)

func TestIntegrationProjectRepository(t *testing.T) {
	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		return dbConn
	}
	ctx := context.Background()

	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	testConfigs := []models.ProjectSpec{
		{
			ID:   models.ProjectID(uuid.New()),
			Name: "g-optimus",
		},
		{
			Name: "",
		},
		{
			ID:   models.ProjectID(uuid.New()),
			Name: "t-optimus",
			Config: map[string]string{
				"bucket":                  "gs://some_folder",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   models.ProjectID(uuid.New()),
			Name: "t-optimus-2",
			Config: map[string]string{
				"bucket":                  "gs://some_folder-2",
				transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
			},
		},
		{
			ID:   models.ProjectID(uuid.New()),
			Name: "t-optimus-3",
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()

		testModels := []models.ProjectSpec{}
		testModels = append(testModels, testConfigs...)

		repo := postgres.NewProjectRepository(db, hash)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		err = repo.Insert(ctx, testModels[1])
		assert.NotNil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()

			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := postgres.NewProjectRepository(db, hash)

			// try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			// try for update
			err = repo.Save(ctx, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelB.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "10.12.12.12:6668,10.12.12.13:6668", checkModel.Config[transporterKafkaBrokerKey])
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()

			testModelA := testConfigs[2]

			repo := postgres.NewProjectRepository(db, hash)

			// try for create
			testModelA.Config["bucket"] = "gs://some_folder"
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			// try for update
			testModelA.Config["bucket"] = "gs://another_folder"
			err = repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Config["bucket"])
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()

			testModelA := testConfigs[0]
			testModelA.ID = models.ProjectID(uuid.Nil)

			repo := postgres.NewProjectRepository(db, hash)

			// try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
		})
		t.Run("should not update empty config", func(t *testing.T) {
			db := DBSetup()

			repo := postgres.NewProjectRepository(db, hash)

			err := repo.Insert(ctx, testConfigs[4])
			assert.Nil(t, err)

			err = repo.Save(ctx, testConfigs[4])
			assert.Equal(t, store.ErrEmptyConfig, err)

			checkModel, err := repo.GetByName(ctx, testConfigs[4].Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-3", checkModel.Name)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()

		testModels := []models.ProjectSpec{}
		testModels = append(testModels, testConfigs...)

		repo := postgres.NewProjectRepository(db, hash)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		err = postgres.NewSecretRepository(db, hash).Save(ctx, testModels[0], models.NamespaceSpec{}, models.ProjectSecretItem{
			Name:  "t1",
			Value: "v1",
		})
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)

		sec, _ := checkModel.Secret.GetByName("t1")
		assert.Equal(t, "v1", sec)
	})
	t.Run("GetAllWithUpstreams", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.ProjectSpec
		testModels = append(testModels, testConfigs...)

		repo := postgres.NewProjectRepository(db, hash)

		assert.Nil(t, repo.Insert(ctx, testModels[2]))
		assert.Nil(t, repo.Insert(ctx, testModels[3]))

		err := postgres.NewSecretRepository(db, hash).Save(ctx, testModels[2], models.NamespaceSpec{}, models.ProjectSecretItem{
			Name:  "t1",
			Value: "v1",
		})
		assert.Nil(t, err)
		err = postgres.NewSecretRepository(db, hash).Save(ctx, testModels[3], models.NamespaceSpec{}, models.ProjectSecretItem{
			Name:  "t2",
			Value: "v2",
		})
		assert.Nil(t, err)

		checkModels, err := repo.GetAll(ctx)
		assert.Nil(t, err)
		sort.Slice(checkModels, func(i, j int) bool {
			return checkModels[i].Name < checkModels[j].Name
		})

		assert.Equal(t, "t-optimus", checkModels[0].Name)
		sec, _ := checkModels[0].Secret.GetByName("t1")
		assert.Equal(t, "v1", sec)

		assert.Equal(t, "t-optimus-2", checkModels[1].Name)
		sec, _ = checkModels[1].Secret.GetByName("t2")
		assert.Equal(t, "v2", sec)
	})
}
