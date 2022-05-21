//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
)

func TestIntegrationJobRepository(t *testing.T) {
	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		return dbConn
	}
	ctx := context.Background()

	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	gTask := "g-task"
	tTask := "t-task"
	jobDestination := "p.d.t"
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
	adapter := postgres.NewAdapter(pluginRepo)

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}
	namespaceSpec2 := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-2",
		ProjectSpec: projectSpec,
	}
	testConfigs := []models.JobSpec{
		{
			ID:   uuid.New(),
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
			ExternalDependencies: models.ExternalDependency{
				HTTPDependencies: []models.HTTPDependency{
					{
						Name: "test_http_sensor_1",
						RequestParams: map[string]string{
							"key_test": "value_test",
						},
						URL: "http://test/optimus/status/1",
						Headers: map[string]string{
							"Content-Type": "application/json",
						},
					},
				},
			},
			NamespaceSpec: namespaceSpec,
		},
		{
			Name:          "",
			NamespaceSpec: namespaceSpec,
		},
		{
			ID:   uuid.New(),
			Name: "t-optimus-id",
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit2, DependencyMod: depMod2},
				Config: []models.JobSpecConfigItem{
					{
						Name: "do", Value: "this",
					},
				},
			},
			NamespaceSpec: namespaceSpec,
		},
	}

	t.Run("Insert", func(t *testing.T) {
		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		t.Run("insert with hooks and assets should return adapted hooks and assets", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err = repo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)

			err = repo.Insert(ctx, testModels[1], jobDestination)
			assert.NotNil(t, err)

			checkModel, err := repo.GetByName(ctx, testModels[0].Name)
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

			defer execUnit1.AssertExpectations(t)
			defer execUnit2.AssertExpectations(t)

			testModels := []models.JobSpec{}
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// first insert
			err := repo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModels[0].Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)

			// soft delete
			err = repo.Delete(ctx, testModels[0].Name)
			assert.Nil(t, err)

			// insert back again
			err = repo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModels[0].Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			defer execUnit1.AssertExpectations(t)

			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: tTask,
			}, nil)
			defer execUnit2.AssertExpectations(t)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// try for create
			err := repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
			assert.Equal(t, gTask, taskSchema.Name)

			// try for update
			err = repo.Save(ctx, testModelB, jobDestination)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelB.Name)
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-id", checkModel.Name)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]

			defer execUnit1.AssertExpectations(t)

			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       tTask,
				PluginType: models.PluginTypeTask,
			}, nil)
			defer execUnit2.AssertExpectations(t)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// try for create
			testModelA.Task.Unit = &models.Plugin{Base: execUnit1, DependencyMod: depMod1}
			err := repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			taskSchema := checkModel.Task.Unit.Info()
			assert.Equal(t, gTask, taskSchema.Name)

			testModelA.Task.Window.Offset = time.Hour * 2
			testModelA.Task.Window.Size = 0

			// try for update
			testModelA.Task.Unit = &models.Plugin{Base: execUnit2, DependencyMod: depMod2}
			err = repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			taskSchema = checkModel.Task.Unit.Info()
			assert.Equal(t, tTask, taskSchema.Name)
			assert.Equal(t, time.Hour*2, checkModel.Task.Window.Offset)
			assert.Equal(t, time.Duration(0), checkModel.Task.Window.Size)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]
			testModelA.ID = uuid.Nil

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// try for create
			err := repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
		})
		t.Run("should update same job with hooks when provided separately", func(t *testing.T) {
			db := DBSetup()
			testModel := testConfigs[2]
			testModel.Task.Unit.DependencyMod = nil
			execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       tTask,
				PluginType: models.PluginTypeTask,
			}, nil)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := repo.Insert(ctx, testModel, jobDestination)
			assert.Nil(t, err)
			checkModel, err := repo.GetByName(ctx, testModel.Name)
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
			err = repo.Save(ctx, testModel, jobDestination)
			assert.Nil(t, err)
			checkModel, err = repo.GetByName(ctx, testModel.Name)
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
			err = repo.Save(ctx, testModel, jobDestination)
			assert.Nil(t, err)
			checkModel, err = repo.GetByName(ctx, testModel.Name)
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
			testModelA := testConfigs[0]

			defer execUnit1.AssertExpectations(t)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepoNamespace1 := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			jobRepoNamespace2 := postgres.NewJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)

			// try to create with first namespace
			err := jobRepoNamespace1.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkJob.Name)
			schema := checkJob.Task.Unit.Info()
			assert.Equal(t, gTask, schema.Name)
			assert.Equal(t, namespaceSpec.ID, checkNamespace.ID)
			assert.Equal(t, namespaceSpec.ProjectSpec.ID, checkNamespace.ProjectSpec.ID)

			// try to create same job with second namespace and it should fail.
			err = jobRepoNamespace2.Save(ctx, testModelA, jobDestination)
			assert.NotNil(t, err)
			assert.Equal(t, "job g-optimus-id already exists for the project t-optimus-id", err.Error())
		})
		t.Run("should properly insert spec behavior, reading and writing", func(t *testing.T) {
			db := DBSetup()
			testModelA := testConfigs[0]

			defer execUnit1.AssertExpectations(t)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			// try for create
			err := repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "g-optimus-id", checkModel.Name)
			assert.Equal(t, true, checkModel.Behavior.CatchUp)
			assert.Equal(t, false, checkModel.Behavior.DependsOnPast)
			assert.Equal(t, 2, checkModel.Behavior.Retry.Count)
			assert.Equal(t, time.Duration(0), checkModel.Behavior.Retry.Delay)
			assert.Equal(t, true, checkModel.Behavior.Retry.ExponentialBackoff)

			// try for update
			testModelA.Behavior.CatchUp = false
			testModelA.Behavior.DependsOnPast = true
			err = repo.Save(ctx, testModelA, jobDestination)
			assert.Nil(t, err)

			checkModel, err = repo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, false, checkModel.Behavior.CatchUp)
			assert.Equal(t, true, checkModel.Behavior.DependsOnPast)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkModel.Name)
		assert.Equal(t, "this", checkModel.Task.Config[0].Value)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)

		checkModels, err := repo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})
}

func TestIntegrationProjectJobRepository(t *testing.T) {
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

	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	gTask := "g-task"
	tTask := "t-task"
	jobDestination := "p.d.t"
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
	adapter := postgres.NewAdapter(pluginRepo)

	testConfigs := []models.JobSpec{
		{
			ID:   uuid.New(),
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
			ID:   uuid.New(),
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
			ID:   uuid.New(),
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
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}
	namespaceSpec2 := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-2",
		ProjectSpec: projectSpec,
	}

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)

		checkJob, checkNamespace, err := projectJobSpecRepo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "g-optimus-id", checkJob.Name)
		assert.Equal(t, "this", checkJob.Task.Config[0].Value)
		assert.Equal(t, namespaceSpec.Name, checkNamespace.Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: tTask,
		}, nil)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})

	t.Run("GetByDestination", func(t *testing.T) {
		db := DBSetup()

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)

		pairs, err := projectJobSpecRepo.GetByDestination(ctx, jobDestination)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, pairs[0].Job.Name)
		assert.Equal(t, projectSpec.Name, pairs[0].Project.Name)
	})

	t.Run("GetByNameForProject", func(t *testing.T) {
		db := DBSetup()

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		assert.Nil(t, postgres.NewProjectRepository(db, hash).Save(ctx, projectSpec))

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)

		j, p, err := projectJobSpecRepo.GetByNameForProject(ctx, projectSpec.Name, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, testConfigs[0].Name, j.Name)
		assert.Equal(t, projectSpec.Name, p.Name)
	})

	t.Run("GetJobNamespaces", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: tTask,
		}, nil)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repoNamespace1 := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repoNamespace1.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repoNamespace1.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)

		repoNamespace2 := postgres.NewJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)
		err = repoNamespace2.Insert(ctx, testModels[3], jobDestination)
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetJobNamespaces(ctx)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []string{testModels[0].Name, testModels[2].Name}, []string{checkModels[namespaceSpec.Name][0], checkModels[namespaceSpec.Name][1]})
		assert.ElementsMatch(t, []string{testModels[3].Name}, []string{checkModels[namespaceSpec2.Name][0]})
	})

	t.Run("GetByIDs", func(t *testing.T) {
		db := DBSetup()
		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: tTask,
		}, nil)

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		repo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[3], jobDestination)
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetByIDs(ctx, []uuid.UUID{testModels[0].ID, testModels[2].ID})
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})
}
