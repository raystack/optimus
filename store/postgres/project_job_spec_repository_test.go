//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
)

func TestIntegrationProjectJobSpecRepository(t *testing.T) {
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
		repo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

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
		repo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repo.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repo.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)

		checkModels, err := projectJobSpecRepo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(checkModels))
	})

	t.Run("GetByNameForProject", func(t *testing.T) {
		db := DBSetup()

		defer execUnit1.AssertExpectations(t)
		defer execUnit2.AssertExpectations(t)

		testModels := []models.JobSpec{}
		testModels = append(testModels, testConfigs...)

		assert.Nil(t, postgres.NewProjectRepository(db, hash).Save(ctx, projectSpec))

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
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
		repoNamespace1 := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := repoNamespace1.Insert(ctx, testModels[0], jobDestination)
		assert.Nil(t, err)
		err = repoNamespace1.Insert(ctx, testModels[2], jobDestination)
		assert.Nil(t, err)

		repoNamespace2 := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec2, projectJobSpecRepo, adapter)
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
		repo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

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
