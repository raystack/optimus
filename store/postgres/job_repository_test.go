// +build !unit_test

package postgres

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/models"
)

func TestJobRepository(t *testing.T) {
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
		return dbConn
	}

	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	testConfigs := []models.JobSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "g-optimus-id",
			Task: models.JobSpecTask{
				Name: "g-task",
				Config: map[string]string{
					"do": "this",
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
		},
		{
			Name: "",
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus-id",
			Task: models.JobSpecTask{
				Name: "t-task",
				Config: map[string]string{
					"do": "this",
				},
			},
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewJobRepository(db, projectSpec)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		err = repo.Insert(testModels[1])
		assert.NotNil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkModel.Name)
		assert.Equal(t, "g-task", checkModel.Task.Name)
		assert.Equal(t, "query.sql", checkModel.Assets.GetAll()[0].Name)
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := NewJobRepository(db, projectSpec)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, "g-task", checkModel.Task.Name)

			//try for update
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, "t-task", checkModel.Task.Name)
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			repo := NewJobRepository(db, projectSpec)

			//try for create
			testModelA.Task.Name = "g-task"
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, "g-task", checkModel.Task.Name)

			//try for update
			testModelA.Task.Name = "t-task"
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-task", checkModel.Task.Name)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			repo := NewJobRepository(db, projectSpec)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewJobRepository(db, projectSpec)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkModel.Name)
		assert.Equal(t, "this", checkModel.Task.Config["do"])
	})
	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewJobRepository(db, projectSpec)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(testModels[2])
		assert.Nil(t, err)

		checkModels, err := repo.GetAll()
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})
}
