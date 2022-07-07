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

func TestUnknownJobDependencyRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	externalProjectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "external-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	gTask := "g-task"
	//tTask := "t-task"
	jobDestination1 := "p.d.t1"
	jobDestination2 := "p.d.t2"
	jobDestinationUnknown := "p.d.t3"

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
	externalProjectNamespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-2",
		ProjectSpec: externalProjectSpec,
	}
	unknownDependencyName := "external-project/external-job"
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
			ResourceDestination: jobDestination1,
			Dependencies: map[string]models.JobSpecDependency{
				unknownDependencyName: {Type: models.JobSpecDependencyTypeInter},
			},
			NamespaceSpec: namespaceSpec,
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
			Dependencies:        map[string]models.JobSpecDependency{},
			ResourceDestination: jobDestination2,
			NamespaceSpec:       namespaceSpec,
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		assert.Nil(t, projRepo.Save(ctx, externalProjectSpec))
		return dbConn
	}

	t.Run("GetUnknownInferredDependencyURNsByJobName", func(t *testing.T) {
		t.Run("GetUnknownInferredDependencyURNsByJobName should return all unknown inferred dependency urns", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			err = namespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination1)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination2)
			assert.Nil(t, err)

			// insert the inferred dependency
			jobSourceRepo := postgres.NewJobSourceRepository(db)
			err = jobSourceRepo.Save(ctx, projectSpec.ID, testModels[0].ID, []string{testModels[1].ResourceDestination, jobDestinationUnknown})
			assert.Nil(t, err)

			// check getting unknown inferred dependent jobs
			repo := postgres.NewUnknownJobDependencyRepository(db)
			checkModel, err := repo.GetUnknownInferredDependencyURNsByJobName(ctx, projectSpec.ID)

			assert.Equal(t, map[string][]string{testModels[0].Name: {jobDestinationUnknown}}, checkModel)
			assert.Nil(t, err)
		})
	})

	t.Run("GetUnknownStaticDependencyNamesByJobName", func(t *testing.T) {
		t.Run("GetUnknownStaticDependencyNamesByJobName should return all unknown static dependency urns", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			err = namespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination1)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination2)
			assert.Nil(t, err)

			repo := postgres.NewUnknownJobDependencyRepository(db)
			checkModel, err := repo.GetUnknownStaticDependencyNamesByJobName(ctx, projectSpec.ID)

			assert.Equal(t, map[string][]string{testModels[0].Name: {unknownDependencyName}}, checkModel)
			assert.Nil(t, err)
		})
	})
}
