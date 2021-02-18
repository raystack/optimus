// +build !unit_test

package postgres

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/mock"
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

	gTask := "g-task"
	tTask := "t-task"
	execUnit1 := new(mock.ExecutionUnit)
	execUnit1.On("GetName").Return(gTask)
	execUnit2 := new(mock.ExecutionUnit)
	execUnit2.On("GetName").Return(tTask)

	gHook := "g-hook"
	hookUnit1 := new(mock.HookUnit)
	hookUnit1.On("GetName").Return(gHook)
	tHook := "g-hook"
	hookUnit2 := new(mock.HookUnit)
	hookUnit2.On("GetName").Return(tHook)

	allTasksRepo := new(mock.SupportedTaskRepo)
	allTasksRepo.On("GetByName", gTask).Return(execUnit1, nil)
	allTasksRepo.On("GetByName", tTask).Return(execUnit2, nil)
	allHooksRepo := new(mock.SupportedHookRepo)
	allHooksRepo.On("GetByName", gHook).Return(hookUnit1, nil)
	allHooksRepo.On("GetByName", tHook).Return(hookUnit2, nil)
	adapter := NewAdapter(allTasksRepo, allHooksRepo)

	testConfigs := []models.JobSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "g-optimus-id",
			Task: models.JobSpecTask{
				Unit: execUnit1,
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
				},
			),
			Hooks: []models.JobSpecHook{
				{
					Type:   models.HookTypePre,
					Config: map[string]string{"FILTER_EXPRESSION": "event_timestamp > 10000"},
					Unit:   hookUnit1,
				},
			},
		},
		{
			Name: "",
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus-id",
			Task: models.JobSpecTask{
				Unit: execUnit2,
				Config: map[string]string{
					"do": "this",
				},
			},
		},
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("insert with hooks and assets should return adapted hooks and assets", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModels := []models.JobSpec{}
			testModels = append(testModels, testConfigs...)

			repo := NewJobRepository(db, projectSpec, adapter)

			err := repo.Insert(testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(testModels[1])
			assert.NotNil(t, err)

			checkModel, err := repo.GetByID(testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, gTask, checkModel.Task.Unit.GetName())
			assert.Equal(t, "query.sql", checkModel.Assets.GetAll()[0].Name)

			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.GetName())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Type)
			assert.Equal(t, "event_timestamp > 10000", checkModel.Hooks[0].Config["FILTER_EXPRESSION"])
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)
			assert.Equal(t, 1, len(checkModel.Hooks))
		})
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := NewJobRepository(db, projectSpec, adapter)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, gTask, checkModel.Task.Unit.GetName())

			//try for update
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.GetName())
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			repo := NewJobRepository(db, projectSpec, adapter)

			//try for create
			testModelA.Task.Unit = execUnit1
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, gTask, checkModel.Task.Unit.GetName())

			//try for update
			testModelA.Task.Unit = execUnit2
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, tTask, checkModel.Task.Unit.GetName())
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			repo := NewJobRepository(db, projectSpec, adapter)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
		t.Run("should update same job with hooks when provided separately", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModel := testConfigs[2]
			repo := NewJobRepository(db, projectSpec, adapter)

			err := repo.Insert(testModel)
			assert.Nil(t, err)
			checkModel, err := repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.GetName())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 0, len(checkModel.Hooks))

			// add a hook and it should be saved and retrievable
			testModel.Hooks = []models.JobSpecHook{
				{
					Type:   models.HookTypePre,
					Config: map[string]string{"FILTER_EXPRESSION": "event_timestamp > 10000"},
					Unit:   hookUnit1,
				},
			}
			err = repo.Save(testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.GetName())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 1, len(checkModel.Hooks))
			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.GetName())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Type)
			assert.Equal(t, "event_timestamp > 10000", checkModel.Hooks[0].Config["FILTER_EXPRESSION"])
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)

			// add one more hook and it should be saved and retrievable
			testModel.Hooks = append(testModel.Hooks, models.JobSpecHook{
				Type:   models.HookTypePre,
				Config: map[string]string{"FILTER_EXPRESSION": "event_timestamp > 10000", "KAFKA_TOPIC": "my_topic.name.kafka"},
				Unit:   hookUnit1,
			})
			err = repo.Save(testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.GetName())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 2, len(checkModel.Hooks))
			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.GetName())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Type)
			assert.Equal(t, "event_timestamp > 10000", checkModel.Hooks[0].Config["FILTER_EXPRESSION"])
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)
			assert.Equal(t, tHook, checkModel.Hooks[1].Unit.GetName())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[1].Type)
			assert.Equal(t, "event_timestamp > 10000", checkModel.Hooks[1].Config["FILTER_EXPRESSION"])
			assert.Equal(t, "my_topic.name.kafka", checkModel.Hooks[1].Config["KAFKA_TOPIC"])
			assert.Equal(t, hookUnit1, checkModel.Hooks[1].Unit)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewJobRepository(db, projectSpec, adapter)

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

		repo := NewJobRepository(db, projectSpec, adapter)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(testModels[2])
		assert.Nil(t, err)

		checkModels, err := repo.GetAll()
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})
}
