// +build !unit_test

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
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
	execUnit1 := new(mock.TaskPlugin)
	execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
		Name: gTask,
	}, nil)
	execUnit2 := new(mock.TaskPlugin)

	gHook := "g-hook"
	hookUnit1 := new(mock.HookPlugin)
	hookUnit1.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
		Name: gHook,
		Type: models.HookTypePre,
	}, nil)
	tHook := "g-hook"
	hookUnit2 := new(mock.HookPlugin)
	hookUnit2.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
		Name: tHook,
		Type: models.HookTypePre,
	}, nil)

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
			Behavior: models.JobSpecBehavior{
				DependsOnPast: false,
				CatchUp:       true,
				Retry: models.JobSpecBehaviorRetry{
					Count:              2,
					Delay:              0,
					ExponentialBackoff: true,
				},
			},
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

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	namespaceSpec2 := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-2",
		ProjectSpec: projectSpec,
	}

	t.Run("Insert", func(t *testing.T) {
		t.Run("insert with hooks and assets should return adapted hooks and assets", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()

			unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			testModels := []models.JobSpec{}
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := repo.Insert(testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(testModels[1])
			assert.NotNil(t, err)

			checkModel, err := repo.GetByID(testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema, _ := checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, gTask, taskSchema.Name)
			assert.Equal(t, "query.sql", checkModel.Assets.GetAll()[0].Name)

			schema, _ := checkModel.Hooks[0].Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.Type)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)
			assert.Equal(t, 1, len(checkModel.Hooks))

			cval, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", cval)
		})
		t.Run("insert when previously soft deleted should hard delete first along with foreign key cascade", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()

			unitData1 := models.GenerateTaskDestinationRequest{
				Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config),
				Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets),
			}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(
				models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			testModels := []models.JobSpec{}
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// first insert
			err := repo.Insert(testModels[0])
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)

			// insert foreign relations
			instanceRepo := NewInstanceRepository(db, testModels[0], adapter)
			err = instanceRepo.Save(models.InstanceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Job:         testModels[0],
				ScheduledAt: time.Date(2021, 5, 10, 2, 2, 0, 0, time.UTC),
				State:       "exploded",
				Data:        nil,
			})
			assert.Nil(t, err)

			// soft delete
			err = repo.Delete(testModels[0].Name)
			assert.Nil(t, err)

			// insert back again
			err = repo.Insert(testModels[0])
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)

			unitData2 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
			execUnit2.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
				Name: tTask,
			}, nil)
			execUnit2.On("GenerateTaskDestination", context.TODO(), unitData2).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit2.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema, _ := checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, gTask, taskSchema.Name)

			//try for update
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema, _ = checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, tTask, taskSchema.Name)
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)

			unitData2 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
			execUnit2.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			execUnit2.On("GenerateTaskDestination", context.TODO(), unitData2).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit2.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			testModelA.Task.Unit = execUnit1
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema, _ := checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, gTask, taskSchema.Name)

			testModelA.Task.Window.Offset = time.Hour * 2
			testModelA.Task.Window.Size = 0

			//try for update
			testModelA.Task.Unit = execUnit2
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			taskSchema, _ = checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, time.Hour*2, checkModel.Task.Window.Offset)
			assert.Equal(t, time.Duration(0), checkModel.Task.Window.Size)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

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

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := repo.Insert(testModel)
			assert.Nil(t, err)
			checkModel, err := repo.GetByID(testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema, _ := checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, tTask, taskSchema.Name)
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
			taskSchema, _ = checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 1, len(checkModel.Hooks))

			schema, _ := checkModel.Hooks[0].Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.Type)

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
			taskSchema, _ = checkModel.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 2, len(checkModel.Hooks))
			schema, _ = checkModel.Hooks[0].Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.Type)

			val1b, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", val1b)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit)

			schema, _ = checkModel.Hooks[1].Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
			assert.Equal(t, tHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.Type)
			assert.Equal(t, hookUnit1, checkModel.Hooks[1].Unit)

			val1, _ := checkModel.Hooks[1].Config.Get("FILTER_EXPRESSION")
			val2, _ := checkModel.Hooks[1].Config.Get("KAFKA_TOPIC")
			assert.Equal(t, "event_timestamp > 10000", val1)
			assert.Equal(t, "my_topic.name.kafka", val2)
		})
		t.Run("should fail if job is already registered for a project with different namespace", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepoNamespace1 := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			jobRepoNamespace2 := NewJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)

			// try to create with first namespace
			err := jobRepoNamespace1.Save(testModelA)
			assert.Nil(t, err)

			checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkJob.Name)
			schema, _ := checkJob.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			assert.Equal(t, gTask, schema.Name)
			assert.Equal(t, namespaceSpec.ID, checkNamespace.ID)
			assert.Equal(t, namespaceSpec.ProjectSpec.ID, checkNamespace.ProjectSpec.ID)

			// try to create same job with second namespace and it should fail.
			err = jobRepoNamespace2.Save(testModelA)
			assert.NotNil(t, err)
			assert.Equal(t, "job g-optimus-id already exists for the project t-optimus-id", err.Error())
		})
		t.Run("should properly insert spec behavior, reading and writing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
			defer execUnit1.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, true, checkModel.Behavior.CatchUp)
			assert.Equal(t, false, checkModel.Behavior.DependsOnPast)
			assert.Equal(t, 2, checkModel.Behavior.Retry.Count)
			assert.Equal(t, time.Duration(0), checkModel.Behavior.Retry.Delay)
			assert.Equal(t, true, checkModel.Behavior.Retry.ExponentialBackoff)

			//try for update
			testModelA.Behavior.CatchUp = false
			testModelA.Behavior.DependsOnPast = true
			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, false, checkModel.Behavior.CatchUp)
			assert.Equal(t, true, checkModel.Behavior.DependsOnPast)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

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

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(testModels[2])
		assert.Nil(t, err)

		checkModels, err := repo.GetAll()
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})
}

func TestProjectJobRepository(t *testing.T) {
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
	execUnit1 := new(mock.TaskPlugin)
	execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
		Name: gTask,
	}, nil)
	execUnit2 := new(mock.TaskPlugin)

	gHook := "g-hook"
	hookUnit1 := new(mock.HookPlugin)
	hookUnit1.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
		Name: gHook,
		Type: models.HookTypePre,
	}, nil)
	tHook := "g-hook"
	hookUnit2 := new(mock.HookPlugin)
	hookUnit2.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
		Name: tHook,
		Type: models.HookTypePre,
	}, nil)

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
				Config: []models.JobSpecConfigItem{
					{
						Name:  "do",
						Value: "this",
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
						Name:  "do",
						Value: "this",
					},
				},
			},
		},
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
		execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkJob.Name)
		assert.Equal(t, "this", checkJob.Task.Config[0].Value)
		assert.Equal(t, namespaceSpec.Name, checkNamespace.Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		unitData1 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
		execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)
		execUnit2.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
			Name: tTask,
		}, nil)
		unitData2 := models.GenerateTaskDestinationRequest{Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
		execUnit2.On("GenerateTaskDestination", context.TODO(), unitData2).Return(models.GenerateTaskDestinationResponse{Destination: destination}, nil)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(testModels[2])
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetAll()
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})

	t.Run("GetByDestination", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

		unitData1 := models.GenerateTaskDestinationRequest{
			Config: models.TaskPluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config),
			Assets: models.TaskPluginAssets{}.FromJobSpec(testConfigs[0].Assets),
		}
		execUnit1.On("GenerateTaskDestination", context.TODO(), unitData1).Return(
			models.GenerateTaskDestinationResponse{Destination: destination}, nil)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(testModels[0])
		assert.Nil(t, err)

		j, p, err := projectJobSpecRepo.GetByDestination(destination)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, j.Name)
		assert.Equal(t, projectSpec.Name, p.Name)
	})
}
