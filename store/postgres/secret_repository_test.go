// +build !unit_test

package postgres

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestSecretRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

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
		assert.Nil(t, projRepo.Save(projectSpec))
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
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.ProjectSecretItem{}
		testModels = append(testModels, testConfigs...)

		repo := NewSecretRepository(db, projectSpec, hash)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		err = repo.Insert(testModels[1])
		assert.NotNil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)

			//try for update
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)
			assert.Equal(t, "super-secret", checkModel.Value)
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[2]

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			testModelA.Value = "gs://some_folder"
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", checkModel.Name)

			//try for update
			testModelA.Value = "gs://another_folder"
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "gs://another_folder", checkModel.Value)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			repo := NewSecretRepository(db, projectSpec, hash)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus", checkModel.Name)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.ProjectSecretItem{}
		testModels = append(testModels, testConfigs...)

		repo := NewSecretRepository(db, projectSpec, hash)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus", checkModel.Name)
	})
}
