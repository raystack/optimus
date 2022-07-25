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
	jobDestination := "p.d.t"
	jobDestination1 := "p.d.t1"
	jobDestination2 := "p.d.t2"

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
				Window: &&models.WindowV1{
					SizeAsDuration:   time.Hour * 24,
					OffsetAsDuration: 0,
					TruncateTo:       "h",
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
			Dependencies:        map[string]models.JobSpecDependency{},
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
				Window: &&models.WindowV1{
					SizeAsDuration:   time.Hour * 24,
					OffsetAsDuration: 0,
					TruncateTo:       "h",
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
				Window: &&models.WindowV1{
					SizeAsDuration:   time.Hour * 24,
					OffsetAsDuration: 0,
					TruncateTo:       "h",
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
		{
			ID:   uuid.New(),
			Name: "job-4",
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
				Window: &&models.WindowV1{
					SizeAsDuration:   time.Hour * 24,
					OffsetAsDuration: 0,
					TruncateTo:       "h",
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
			Dependencies:        map[string]models.JobSpecDependency{},
			ResourceDestination: jobDestination2,
			NamespaceSpec:       externalProjectNamespaceSpec,
		},
	}
	testConfigs[2].Dependencies = map[string]models.JobSpecDependency{
		testConfigs[1].Name: {},
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

	t.Run("GetByJobName", func(t *testing.T) {
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
		t.Run("GetDependentJobs should return inferred and static dependent jobs", func(t *testing.T) {
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

		t.Run("GetDependentJobs should return static dependent jobs from same project if the project is not specified in the dependency list", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			externalNamespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err = externalNamespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			// prepare the project's jobs
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

			// prepare the external project's jobs
			externalProjectJobs := []models.JobSpec{
				{
					ID:            uuid.New(),
					Name:          testModels[1].Name,
					NamespaceSpec: externalProjectNamespaceSpec,
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
				},
				{
					ID:            uuid.New(),
					Name:          testModels[2].Name,
					NamespaceSpec: externalProjectNamespaceSpec,
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
					Dependencies: map[string]models.JobSpecDependency{
						testModels[1].Name: {},
					},
				},
			}
			externalProjectJobDestination := "x.d.t"
			externalProjectJobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, externalProjectNamespaceSpec, projectJobSpecRepo, adapter)
			err = externalProjectJobSpecRepo.Insert(ctx, externalProjectJobs[0], externalProjectJobDestination)
			assert.Nil(t, err)
			err = externalProjectJobSpecRepo.Insert(ctx, externalProjectJobs[1], "")
			assert.Nil(t, err)

			// check getting inferred dependent jobs
			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetDependentJobs(ctx, &testModels[0])
			assert.Nil(t, err)
			assert.Equal(t, testModels[1].Name, checkModel[0].Name)

			// check getting static dependent jobs
			checkModel, err = repo.GetDependentJobs(ctx, &testModels[1])
			assert.Nil(t, err)
			assert.Equal(t, 1, len(checkModel))
			assert.Equal(t, testModels[2].Name, checkModel[0].Name)
			assert.Equal(t, testModels[2].ID, checkModel[0].ID)
		})
	})

	t.Run("GetInferredDependenciesPerJobID", func(t *testing.T) {
		t.Run("GetInferredDependenciesPerJobID should return all intra and inter project inferred dependencies", func(t *testing.T) {
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
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			externalJobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, externalProjectNamespaceSpec, projectJobSpecRepo, adapter)
			err = externalJobSpecRepo.Insert(ctx, testModels[3], jobDestination2)
			assert.Nil(t, err)

			// insert the inferred dependency
			jobSourceRepo := postgres.NewJobSourceRepository(db)
			err = jobSourceRepo.Save(ctx, projectSpec.ID, testModels[0].ID, []string{testModels[1].ResourceDestination, testModels[3].ResourceDestination})
			assert.Nil(t, err)

			// check getting inferred dependent jobs
			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetInferredDependenciesPerJobID(ctx, projectSpec.ID)

			jobWithDependency := testModels[0]
			intraDependency := testModels[1]
			interDependency := testModels[3]
			assert.EqualValues(t, []string{intraDependency.Name, interDependency.Name}, []string{checkModel[jobWithDependency.ID][0].Name, checkModel[jobWithDependency.ID][1].Name})
			assert.EqualValues(t, []string{intraDependency.NamespaceSpec.Name, interDependency.NamespaceSpec.Name}, []string{checkModel[jobWithDependency.ID][0].NamespaceSpec.Name, checkModel[jobWithDependency.ID][1].NamespaceSpec.Name})
			assert.EqualValues(t, []string{intraDependency.GetProjectSpec().Name, interDependency.GetProjectSpec().Name}, []string{checkModel[jobWithDependency.ID][0].GetProjectSpec().Name, checkModel[jobWithDependency.ID][1].GetProjectSpec().Name})
			assert.EqualValues(t, []string{jobDestination1, jobDestination2}, []string{checkModel[jobWithDependency.ID][0].ResourceDestination, checkModel[jobWithDependency.ID][1].ResourceDestination})
			assert.EqualValues(t, []string{gTask, gTask}, []string{checkModel[jobWithDependency.ID][0].Task.Unit.Info().Name, checkModel[jobWithDependency.ID][1].Task.Unit.Info().Name})
			assert.Nil(t, err)
		})
	})

	t.Run("GetStaticDependenciesPerJobID", func(t *testing.T) {
		t.Run("GetStaticDependenciesPerJobID should return all intra and inter project static dependencies", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			testModels[0].Dependencies = map[string]models.JobSpecDependency{
				testModels[1].Name: {Project: &testModels[1].NamespaceSpec.ProjectSpec, Job: &testModels[1]},
				testModels[3].Name: {Project: &testModels[3].NamespaceSpec.ProjectSpec, Job: &testModels[3], Type: models.JobSpecDependencyTypeInter},
			}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			err = namespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			externalJobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, externalProjectNamespaceSpec, projectJobSpecRepo, adapter)
			err = externalJobSpecRepo.Insert(ctx, testModels[3], jobDestination2)
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetStaticDependenciesPerJobID(ctx, projectSpec.ID)

			jobWithDependency := testModels[0]
			intraDependency := testModels[1]
			interDependency := testModels[3]
			assert.EqualValues(t, []string{intraDependency.Name, interDependency.Name}, []string{checkModel[jobWithDependency.ID][0].Name, checkModel[jobWithDependency.ID][1].Name})
			assert.EqualValues(t, []string{intraDependency.NamespaceSpec.Name, interDependency.NamespaceSpec.Name}, []string{checkModel[jobWithDependency.ID][0].NamespaceSpec.Name, checkModel[jobWithDependency.ID][1].NamespaceSpec.Name})
			assert.EqualValues(t, []string{intraDependency.GetProjectSpec().Name, interDependency.GetProjectSpec().Name}, []string{checkModel[jobWithDependency.ID][0].GetProjectSpec().Name, checkModel[jobWithDependency.ID][1].GetProjectSpec().Name})
			assert.EqualValues(t, []string{gTask, gTask}, []string{checkModel[jobWithDependency.ID][0].Task.Unit.Info().Name, checkModel[jobWithDependency.ID][1].Task.Unit.Info().Name})
			assert.Nil(t, err)
		})
		t.Run("GetStaticDependenciesPerJobID should return only the dependencies that are found in internal server", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			testModels[0].Dependencies = map[string]models.JobSpecDependency{
				testModels[1].Name: {Project: &testModels[1].NamespaceSpec.ProjectSpec, Job: &testModels[1]},
				testModels[3].Name: {Project: &testModels[3].NamespaceSpec.ProjectSpec, Job: &testModels[3], Type: models.JobSpecDependencyTypeInter},
				"external-server-project/external-server-job": {Type: models.JobSpecDependencyTypeInter},
			}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			err = namespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			externalJobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, externalProjectNamespaceSpec, projectJobSpecRepo, adapter)
			err = externalJobSpecRepo.Insert(ctx, testModels[3], jobDestination2)
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetStaticDependenciesPerJobID(ctx, projectSpec.ID)

			jobWithDependency := testModels[0]
			intraDependency := testModels[1]
			interDependency := testModels[3]
			noOfInternalDependencies := 2
			assert.Equal(t, noOfInternalDependencies, len(checkModel[jobWithDependency.ID]))
			assert.EqualValues(t, []string{intraDependency.Name, interDependency.Name}, []string{checkModel[jobWithDependency.ID][0].Name, checkModel[jobWithDependency.ID][1].Name})
			assert.EqualValues(t, []string{intraDependency.NamespaceSpec.Name, interDependency.NamespaceSpec.Name}, []string{checkModel[jobWithDependency.ID][0].NamespaceSpec.Name, checkModel[jobWithDependency.ID][1].NamespaceSpec.Name})
			assert.EqualValues(t, []string{intraDependency.GetProjectSpec().Name, interDependency.GetProjectSpec().Name}, []string{checkModel[jobWithDependency.ID][0].GetProjectSpec().Name, checkModel[jobWithDependency.ID][1].GetProjectSpec().Name})
			assert.EqualValues(t, []string{gTask, gTask}, []string{checkModel[jobWithDependency.ID][0].Task.Unit.Info().Name, checkModel[jobWithDependency.ID][1].Task.Unit.Info().Name})
			assert.Nil(t, err)
		})
		t.Run("GetStaticDependenciesPerJobID should return only the dependencies in the same project if project dependency is not set", func(t *testing.T) {
			db := DBSetup()

			defer execUnit1.AssertExpectations(t)
			var testModels []models.JobSpec
			testModels = append(testModels, testConfigs...)

			testModels[0].Dependencies = map[string]models.JobSpecDependency{
				testModels[1].Name: {Project: &testModels[1].NamespaceSpec.ProjectSpec, Job: &testModels[1]},
			}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			// prepare internal project job specs
			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			jobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobSpecRepo.Insert(ctx, testModels[0], jobDestination)
			assert.Nil(t, err)
			err = jobSpecRepo.Insert(ctx, testModels[1], jobDestination1)
			assert.Nil(t, err)

			// prepare external project job specs
			externalNamespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err = externalNamespaceRepo.Insert(ctx, externalProjectSpec, externalProjectNamespaceSpec)
			assert.Nil(t, err)

			externalProjectJobs := []models.JobSpec{
				{
					ID:            uuid.New(),
					Name:          testModels[0].Name,
					NamespaceSpec: externalProjectNamespaceSpec,
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
				},
				{
					ID:            uuid.New(),
					Name:          testModels[1].Name,
					NamespaceSpec: externalProjectNamespaceSpec,
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
				},
			}
			externalProjectJobs[0].Dependencies = map[string]models.JobSpecDependency{
				externalProjectJobs[1].Name: {Project: &externalProjectJobs[1].NamespaceSpec.ProjectSpec, Job: &externalProjectJobs[1]},
			}

			externalJobSpecRepo := postgres.NewNamespaceJobSpecRepository(db, externalProjectNamespaceSpec, projectJobSpecRepo, adapter)
			err = externalJobSpecRepo.Insert(ctx, externalProjectJobs[0], "")
			assert.Nil(t, err)
			err = externalJobSpecRepo.Insert(ctx, externalProjectJobs[1], "")
			assert.Nil(t, err)

			repo := postgres.NewJobSpecRepository(db, adapter)
			checkModel, err := repo.GetStaticDependenciesPerJobID(ctx, projectSpec.ID)

			jobWithDependency := testModels[0]
			intraDependency := testModels[1]
			noOfInternalDependencies := 1
			assert.Equal(t, noOfInternalDependencies, len(checkModel[jobWithDependency.ID]))
			assert.EqualValues(t, []string{intraDependency.Name}, []string{checkModel[jobWithDependency.ID][0].Name})
			assert.EqualValues(t, []string{intraDependency.NamespaceSpec.Name}, []string{checkModel[jobWithDependency.ID][0].NamespaceSpec.Name})
			assert.EqualValues(t, []string{intraDependency.GetProjectSpec().Name}, []string{checkModel[jobWithDependency.ID][0].GetProjectSpec().Name})
			assert.Nil(t, err)
		})
	})
}
