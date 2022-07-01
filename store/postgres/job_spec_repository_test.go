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

func TestJobSpecRepository(t *testing.T) {
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
	//tTask := "t-task"
	jobDestination := "p.d.t"
	jobDestination1 := "p.d.t1"

	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       gTask,
		PluginType: models.PluginTypeTask,
	}, nil)

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1}, nil)
	adapter := postgres.NewAdapter(pluginRepo)
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}
	testConfigs := []models.JobSpec{
		{
			ID:   uuid.New(),
			Name: "job-1",
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
				Unit: &models.Plugin{Base: execUnit1},
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
			ResourceDestination: jobDestination,
			NamespaceSpec:       namespaceSpec,
		},
		{
			ID:   uuid.New(),
			Name: "job-2",
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
				Unit: &models.Plugin{Base: execUnit1},
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
			ResourceDestination: jobDestination1,
			NamespaceSpec:       namespaceSpec,
		},
		{
			ID:   uuid.New(),
			Name: "job-3",
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
				Unit: &models.Plugin{Base: execUnit1},
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
			NamespaceSpec: namespaceSpec,
		},
	}
	testConfigs[2].Dependencies = map[string]models.JobSpecDependency{
		testConfigs[1].Name: {},
	}

	t.Run("GetByJobName", func(t *testing.T) {
		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		t.Run("GetByJobName should return expected job for specified jobName filter", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			jobspecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobspecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobspecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetJobByName(ctx, testModels[0].Name)
			assert.Nil(t, err)
			assert.Equal(t, testModels[0].Name, checkModel[0].Name)

			checkModel1, err := repo.GetJobByName(ctx, testModels[1].Name)
			assert.Nil(t, err)
			assert.Equal(t, testModels[1].Name, checkModel1[0].Name)
		})
	})

	t.Run("GetByResourceDestination", func(t *testing.T) {
		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		t.Run("GetByResourceDestination should return expected job for specified resourceDestination filter", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			jobspecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobspecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobspecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetJobByResourceDestination(ctx, jobDestination)
			assert.Nil(t, err)
			assert.Equal(t, testModels[0].Name, checkModel.Name)
		})
	})

	t.Run("GetDependentJobs", func(t *testing.T) {
		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		t.Run("GetDependentJobs should return inferred dependent jobs", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[2], "")
			assert.Nil(t, err)

			// insert the inferred dependency
			jobSourceRepo := postgres.NewJobSourceRepository(db)
			err = jobSourceRepo.Save(ctx, projectSpec.ID, testModels[1].ID, []string{testModels[0].ResourceDestination})
			assert.Nil(t, err)

			// check getting inferred dependent jobs
			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetDependentJobs(ctx, &testModels[0])
			assert.Nil(t, err)
			assert.Equal(t, testModels[1].Name, checkModel[0].Name)

			// check getting static dependent jobs
			checkModel, err = repo.GetDependentJobs(ctx, &testModels[1])
			assert.Nil(t, err)
			assert.Equal(t, testModels[2].Name, checkModel[0].Name)
		})
	})
}
