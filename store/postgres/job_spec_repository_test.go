// +build !unit_test

package postgres

import (
	"os"
	"testing"
	"time"

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
	destination := "p.d.t"
	execUnit1 := new(mock.Transformer)
	execUnit1.On("Name").Return(gTask)
	execUnit2 := new(mock.Transformer)

	gHook := "g-hook"
	hookUnit1 := new(mock.HookUnit)
	hookUnit1.On("Name").Return(gHook)
	hookUnit1.On("Type").Return(models.HookTypePre)
	tHook := "g-hook"
	hookUnit2 := new(mock.HookUnit)
	hookUnit2.On("Name").Return(tHook)
	hookUnit2.On("Type").Return(models.HookTypePre)

	allTasksRepo := new(mock.SupportedTransformationRepo)
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
				Config: []models.JobSpecConfigItem{
					{
						Name: "do", Value: "this",
					},
				},
				Window: models.JobSpecTaskWindow{
					Size:       time.Hour * 24,
					Offset:     0,
					TruncateTo: "h",
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
					Config: []models.JobSpecConfigItem{
						{
							Name:  "FILTER_EXPRESSION",
							Value: "event_timestamp > 10000",
						},
					},
					Unit: hookUnit1,
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
				Config: []models.JobSpecConfigItem{
					{
						Name: "do", Value: "this",
					},
				},
			},
		},
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("insert with hooks and assets should return adapted hooks and assets", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()

			unitData1 := models.GenerateDestinationRequest{Config: testConfigs[0].Task.Config, Assets: testConfigs[0].Assets.ToMap()}
			execUnit1.On("GenerateDestination", unitData1).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

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
			assert.Equal(t, gTask, checkModel.Task.Unit.Name())
			assert.Equal(t, "query.sql", checkModel.Assets.GetAll()[0].Name)

			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.Name())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Unit.Type())
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)
			assert.Equal(t, 1, len(checkModel.Hooks))

			cval, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", cval)
		})
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			unitData1 := models.GenerateDestinationRequest{Config: testConfigs[0].Task.Config, Assets: testConfigs[0].Assets.ToMap()}
			execUnit1.On("GenerateDestination", unitData1).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)

			unitData2 := models.GenerateDestinationRequest{Config: testConfigs[2].Task.Config, Assets: testConfigs[2].Assets.ToMap()}
			execUnit2.On("Name").Return(tTask)
			execUnit2.On("GenerateDestination", unitData2).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			defer execUnit2.AssertExpectations(t)

			repo := NewJobRepository(db, projectSpec, adapter)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, gTask, checkModel.Task.Unit.Name())

			//try for update
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.Name())
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateDestinationRequest{Config: testConfigs[0].Task.Config, Assets: testConfigs[0].Assets.ToMap()}
			execUnit1.On("GenerateDestination", unitData1).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			execUnit1.On("Name").Return(tTask)
			defer execUnit1.AssertExpectations(t)

			unitData2 := models.GenerateDestinationRequest{Config: testConfigs[2].Task.Config, Assets: testConfigs[2].Assets.ToMap()}
			execUnit2.On("GenerateDestination", unitData1).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			execUnit2.On("GenerateDestination", unitData2).Return(models.GenerateDestinationResponse{Destination: destination}, nil)
			defer execUnit2.AssertExpectations(t)

			repo := NewJobRepository(db, projectSpec, adapter)

			//try for create
			testModelA.Task.Unit = execUnit1
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, gTask, checkModel.Task.Unit.Name())

			testModelA.Task.Window.Offset = time.Hour * 2
			testModelA.Task.Window.Size = 0

			//try for update
			testModelA.Task.Unit = execUnit2
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, tTask, checkModel.Task.Unit.Name())
			assert.Equal(t, time.Hour*2, checkModel.Task.Window.Offset)
			assert.Equal(t, time.Duration(0), checkModel.Task.Window.Size)
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
			assert.Equal(t, tTask, checkModel.Task.Unit.Name())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 0, len(checkModel.Hooks))

			// add a hook and it should be saved and retrievable
			testModel.Hooks = []models.JobSpecHook{
				{
					Config: []models.JobSpecConfigItem{
						{
							Name:  "FILTER_EXPRESSION",
							Value: "event_timestamp > 10000",
						},
					},
					Unit: hookUnit1,
				},
			}
			err = repo.Save(testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.Name())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 1, len(checkModel.Hooks))
			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.Name())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Unit.Type())

			val1a, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", val1a)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)

			// add one more hook and it should be saved and retrievable
			testModel.Hooks = append(testModel.Hooks, models.JobSpecHook{
				Config: []models.JobSpecConfigItem{
					{
						Name:  "FILTER_EXPRESSION",
						Value: "event_timestamp > 10000",
					},
					{
						Name:  "KAFKA_TOPIC",
						Value: "my_topic.name.kafka",
					},
				},
				Unit: hookUnit1,
			})
			err = repo.Save(testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			assert.Equal(t, tTask, checkModel.Task.Unit.Name())
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 2, len(checkModel.Hooks))
			assert.Equal(t, gHook, checkModel.Hooks[0].Unit.Name())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[0].Unit.Type())

			val1b, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", val1b)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)
			assert.Equal(t, tHook, checkModel.Hooks[1].Unit.Name())
			assert.Equal(t, models.HookTypePre, checkModel.Hooks[1].Unit.Type())
			assert.Equal(t, hookUnit1, checkModel.Hooks[1].Unit)

			val1, _ := checkModel.Hooks[1].Config.Get("FILTER_EXPRESSION")
			val2, _ := checkModel.Hooks[1].Config.Get("KAFKA_TOPIC")
			assert.Equal(t, "event_timestamp > 10000", val1)
			assert.Equal(t, "my_topic.name.kafka", val2)
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
		assert.Equal(t, "this", checkModel.Task.Config[0].Value)
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

	t.Run("GetByDestination", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewJobRepository(db, projectSpec, adapter)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		j, p, err := repo.GetByDestination(destination)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, j.Name)
		assert.Equal(t, projectSpec.Name, p.Name)
	})
}
