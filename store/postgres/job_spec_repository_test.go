//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestJobRepository(t *testing.T) {
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
		return dbConn
	}
	ctx := context.Background()

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
	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       gTask,
		PluginType: models.PluginTypeTask,
	}, nil)
	execUnit2 := new(mock.BasePlugin)

	depMod1 := new(mock.DependencyResolverMod)
	depMod2 := new(mock.DependencyResolverMod)

	gHook := "g-hook"
	hookUnit1 := new(mock.BasePlugin)
	hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       gHook,
		PluginType: models.PluginTypeHook,
		HookType:   models.HookTypePre,
	}, nil)
	tHook := "t-hook"
	hookUnit2 := new(mock.BasePlugin)
	hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       tHook,
		PluginType: models.PluginTypeHook,
		HookType:   models.HookTypePre,
	}, nil)

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
	pluginRepo.On("GetByName", tTask).Return(&models.Plugin{Base: execUnit2, DependencyMod: depMod2}, nil)
	pluginRepo.On("GetByName", gHook).Return(&models.Plugin{Base: hookUnit1}, nil)
	pluginRepo.On("GetByName", tHook).Return(&models.Plugin{Base: hookUnit2}, nil)
	adapter := NewAdapter(pluginRepo)

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
				Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1},
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
					Unit: &models.Plugin{Base: hookUnit1},
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
				Unit: &models.Plugin{Base: execUnit2, DependencyMod: depMod2},
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
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination}, nil)
			defer depMod1.AssertExpectations(t)
			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)

			err = repo.Insert(ctx, testModels[1])
			assert.NotNil(t, err)

			checkModel, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
			assert.Equal(t, gTask, taskSchema.Name)
			assert.Equal(t, "query.sql", checkModel.Assets.GetAll()[0].Name)

			schema := checkModel.Hooks[0].Unit.Info()
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.HookType)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit.Base)
			assert.Equal(t, 1, len(checkModel.Hooks))

			cval, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", cval)
		})
		t.Run("insert when previously soft deleted should hard delete first along with foreign key cascade", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()

			unitData1 := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(
				&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer depMod1.AssertExpectations(t)
			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			testModels := []models.JobSpec{}
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// first insert
			err := repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)

			// soft delete
			err = repo.Delete(ctx, testModels[0].Name)
			assert.Nil(t, err)

			// insert back again
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer depMod1.AssertExpectations(t)
			defer execUnit1.AssertExpectations(t)

			unitData2 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: tTask,
			}, nil)
			depMod2.On("GenerateDestination", context.TODO(), unitData2).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer execUnit2.AssertExpectations(t)
			defer depMod2.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
			assert.Equal(t, gTask, taskSchema.Name)

			//try for update
			err = repo.Save(ctx, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer execUnit1.AssertExpectations(t)
			defer depMod1.AssertExpectations(t)

			depMod2.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       tTask,
				PluginType: models.PluginTypeTask,
			}, nil)
			defer execUnit2.AssertExpectations(t)
			defer depMod2.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			testModelA.Task.Unit = &models.Plugin{Base: execUnit1, DependencyMod: depMod1}
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
			assert.Equal(t, gTask, taskSchema.Name)

			testModelA.Task.Window.Offset = time.Hour * 2
			testModelA.Task.Window.Size = 0

			//try for update
			testModelA.Task.Unit = &models.Plugin{Base: execUnit2, DependencyMod: depMod2}
			err = repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, time.Hour*2, checkModel.Task.Window.Offset)
			assert.Equal(t, time.Duration(0), checkModel.Task.Window.Size)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
		t.Run("should update same job with hooks when provided separately", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModel := testConfigs[2]
			testModel.Task.Unit.DependencyMod = nil
			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       tTask,
				PluginType: models.PluginTypeTask,
			}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := repo.Insert(ctx, testModel)
			assert.Nil(t, err)
			checkModel, err := repo.GetByID(ctx, testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
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
					Unit: &models.Plugin{Base: hookUnit1},
				},
			}
			err = repo.Save(ctx, testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(ctx, testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 1, len(checkModel.Hooks))

			schema := checkModel.Hooks[0].Unit.Info()
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.HookType)

			val1a, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", val1a)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit.Base)

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
				Unit: &models.Plugin{Base: hookUnit2},
			})
			err = repo.Save(ctx, testModel)
			assert.Nil(t, err)
			checkModel, err = repo.GetByID(ctx, testModel.ID)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, 0, len(checkModel.Assets.GetAll()))
			assert.Equal(t, 2, len(checkModel.Hooks))
			schema = checkModel.Hooks[0].Unit.Info()
			assert.Equal(t, gHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.HookType)

			val1b, _ := checkModel.Hooks[0].Config.Get("FILTER_EXPRESSION")
			assert.Equal(t, "event_timestamp > 10000", val1b)
			assert.Equal(t, hookUnit1, checkModel.Hooks[0].Unit.Base)

			schema = checkModel.Hooks[1].Unit.Info()
			assert.Equal(t, tHook, schema.Name)
			assert.Equal(t, models.HookTypePre, schema.HookType)
			assert.Equal(t, hookUnit2, checkModel.Hooks[1].Unit.Base)

			val1, _ := checkModel.Hooks[1].Config.Get("FILTER_EXPRESSION")
			val2, _ := checkModel.Hooks[1].Config.Get("KAFKA_TOPIC")
			assert.Equal(t, "event_timestamp > 10000", val1)
			assert.Equal(t, "my_topic.name.kafka", val2)
		})
		t.Run("should fail if job is already registered for a project with different namespace", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer execUnit1.AssertExpectations(t)
			defer depMod1.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepoNamespace1 := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			jobRepoNamespace2 := NewJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)

			// try to create with first namespace
			err := jobRepoNamespace1.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkJob.Name)
			schema := checkJob.Task.Unit.Info()
			assert.Equal(t, gTask, schema.Name)
			assert.Equal(t, namespaceSpec.ID, checkNamespace.ID)
			assert.Equal(t, namespaceSpec.ProjectSpec.ID, checkNamespace.ProjectSpec.ID)

			// try to create same job with second namespace and it should fail.
			err = jobRepoNamespace2.Save(ctx, testModelA)
			assert.NotNil(t, err)
			assert.Equal(t, "job g-optimus-id already exists for the project t-optimus-id", err.Error())
		})
		t.Run("should properly insert spec behavior, reading and writing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]

			unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
			depMod1.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
			defer execUnit1.AssertExpectations(t)
			defer depMod1.AssertExpectations(t)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
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
			err = repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, false, checkModel.Behavior.CatchUp)
			assert.Equal(t, true, checkModel.Behavior.DependsOnPast)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkModel.Name)
		assert.Equal(t, "this", checkModel.Task.Config[0].Value)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2])
		assert.Nil(t, err)

		checkModels, err := repo.GetAll(ctx)
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
		return dbConn
	}
	ctx := context.Background()

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
	destinationUrn := "bigquery://p.d.t"
	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: gTask,
	}, nil)
	execUnit2 := new(mock.BasePlugin)

	gHook := "g-hook"
	hookUnit1 := new(mock.BasePlugin)
	hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       gHook,
		PluginType: models.PluginTypeHook,
		HookType:   models.HookTypePre,
	}, nil)
	tHook := "g-hook"
	hookUnit2 := new(mock.BasePlugin)
	hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       tHook,
		PluginType: models.PluginTypeHook,
		HookType:   models.HookTypePre,
	}, nil)

	depMod := new(mock.DependencyResolverMod)
	depMod2 := new(mock.DependencyResolverMod)

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod}, nil)
	pluginRepo.On("GetByName", tTask).Return(&models.Plugin{Base: execUnit2, DependencyMod: depMod2}, nil)
	pluginRepo.On("GetByName", gHook).Return(&models.Plugin{Base: hookUnit1}, nil)
	pluginRepo.On("GetByName", tHook).Return(&models.Plugin{Base: hookUnit2}, nil)
	adapter := NewAdapter(pluginRepo)

	testConfigs := []models.JobSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "g-optimus-id",
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod},
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
					Unit: &models.Plugin{Base: hookUnit1},
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
				Unit: &models.Plugin{Base: execUnit2, DependencyMod: depMod2},
				Config: []models.JobSpecConfigItem{
					{
						Name:  "do",
						Value: "this",
					},
				},
			},
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "p-optimus-id",
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit2, DependencyMod: depMod2},
				Config: []models.JobSpecConfigItem{
					{
						Name:  "do",
						Value: "this",
					},
				},
			},
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
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

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
		depMod.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)

		defer depMod.AssertExpectations(t)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkJob.Name)
		assert.Equal(t, "this", checkJob.Task.Config[0].Value)
		assert.Equal(t, namespaceSpec.Name, checkNamespace.Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
		depMod.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)

		execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: tTask,
		}, nil)
		unitData2 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
		depMod2.On("GenerateDestination", context.TODO(), unitData2).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)

		defer depMod.AssertExpectations(t)
		defer depMod2.AssertExpectations(t)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2])
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})

	t.Run("GetByDestination", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()

		unitData1 := models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets),
		}
		depMod.On("GenerateDestination", ctx, unitData1).Return(
			&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
		defer depMod.AssertExpectations(t)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		pairs, err := projectJobSpecRepo.GetByDestination(ctx, destinationUrn)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, pairs[0].Job.Name)
		assert.Equal(t, projectSpec.Name, pairs[0].Project.Name)
	})

	t.Run("GetByNameForProject", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()

		unitData1 := models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets),
		}
		depMod.On("GenerateDestination", context.TODO(), unitData1).Return(
			&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)
		defer depMod.AssertExpectations(t)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		assert.Nil(t, NewProjectRepository(db, hash).Save(ctx, projectSpec))

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		j, p, err := projectJobSpecRepo.GetByNameForProject(ctx, projectSpec.Name, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, j.Name)
		assert.Equal(t, projectSpec.Name, p.Name)
	})

	t.Run("GetJobNamespaces", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		unitData1 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[0].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[0].Assets)}
		depMod.On("GenerateDestination", context.TODO(), unitData1).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)

		execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: tTask,
		}, nil)
		unitData2 := models.GenerateDestinationRequest{Config: models.PluginConfigs{}.FromJobSpec(testConfigs[2].Task.Config), Assets: models.PluginAssets{}.FromJobSpec(testConfigs[2].Assets)}
		depMod2.On("GenerateDestination", context.TODO(), unitData2).Return(&models.GenerateDestinationResponse{Destination: destination, Type: models.DestinationTypeBigquery}, nil)

		defer depMod.AssertExpectations(t)
		defer depMod2.AssertExpectations(t)
		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		repoNamespace1 := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repoNamespace1.Insert(ctx, testModels[0])
		assert.Nil(t, err)
		err = repoNamespace1.Insert(ctx, testModels[2])
		assert.Nil(t, err)

		repoNamespace2 := NewJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)
		err = repoNamespace2.Insert(ctx, testModels[3])
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetJobNamespaces(ctx)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []string{testModels[0].Name, testModels[2].Name}, []string{checkModels[namespaceSpec.Name][0], checkModels[namespaceSpec.Name][1]})
		assert.ElementsMatch(t, []string{testModels[3].Name}, []string{checkModels[namespaceSpec2.Name][0]})
	})
}
