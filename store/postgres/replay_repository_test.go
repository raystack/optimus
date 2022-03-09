//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/core/tree"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func treeIsEqual(treeNode *tree.TreeNode, treeNodeComparator *tree.TreeNode) bool {
	if treeNode.Data.GetName() != treeNodeComparator.Data.GetName() {
		return false
	}
	for idx, dependent := range treeNode.Dependents {
		if !treeIsEqual(dependent, treeNodeComparator.Dependents[idx]) {
			return false
		}
	}
	for idx, run := range treeNode.Runs.Values() {
		if run.(time.Time) != treeNodeComparator.Runs.Values()[idx].(time.Time) {
			return false
		}
	}
	return true
}

func TestIntegrationReplayRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		Secret: []models.ProjectSecretItem{
			{
				ID:    uuid.Must(uuid.NewRandom()),
				Name:  "k1",
				Value: "v1",
			},
		},
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	gTask := "g-task"
	jobConfigs := []models.JobSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "job-1",
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "job-2",
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "job-3",
		},
	}
	startTime := time.Date(2021, 1, 15, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2021, 1, 20, 0, 0, 0, 0, time.UTC)
	run1 := time.Date(2021, 1, 15, 2, 0, 0, 0, time.UTC)
	run2 := time.Date(2021, 1, 16, 2, 0, 0, 0, time.UTC)
	run3 := time.Date(2021, 1, 17, 2, 0, 0, 0, time.UTC)
	run4 := time.Date(2021, 1, 18, 2, 0, 0, 0, time.UTC)
	run5 := time.Date(2021, 1, 19, 2, 0, 0, 0, time.UTC)
	run6 := time.Date(2021, 1, 20, 2, 0, 0, 0, time.UTC)

	treeNode3 := tree.NewTreeNode(jobConfigs[2])
	treeNode3.Runs.Add(run1)
	treeNode3.Runs.Add(run2)
	treeNode3.Runs.Add(run3)
	treeNode3.Runs.Add(run4)
	treeNode3.Runs.Add(run5)
	treeNode3.Runs.Add(run6)

	treeNode2 := tree.NewTreeNode(jobConfigs[1])
	treeNode2.Runs.Add(run1)
	treeNode2.Runs.Add(run2)
	treeNode2.Runs.Add(run3)
	treeNode2.Runs.Add(run4)
	treeNode2.Runs.Add(run5)
	treeNode2.Runs.Add(run6)
	treeNode2.AddDependent(treeNode3)

	treeNode1 := tree.NewTreeNode(jobConfigs[0])
	treeNode1.Runs.Add(run1)
	treeNode1.Runs.Add(run2)
	treeNode1.Runs.Add(run3)
	treeNode1.Runs.Add(run4)
	treeNode1.Runs.Add(run5)
	treeNode1.Runs.Add(run6)
	treeNode1.AddDependent(treeNode2)

	testConfigs := []*models.ReplaySpec{
		{
			ID:            uuid.Must(uuid.NewRandom()),
			StartDate:     startTime,
			EndDate:       endTime,
			Status:        models.ReplayStatusAccepted,
			ExecutionTree: treeNode1,
			Config:        map[string]string{models.ConfigIgnoreDownstream: "true"},
		},
		{
			ID:        uuid.Must(uuid.NewRandom()),
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusFailed,
		},
		{
			ID:        uuid.Must(uuid.NewRandom()),
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusInProgress,
		},
	}

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
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	t.Run("Insert and GetByID", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()

		execUnit1 := new(mock.BasePlugin)
		defer execUnit1.AssertExpectations(t)
		execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: gTask,
		}, nil)
		depMod1 := new(mock.DependencyResolverMod)
		defer depMod1.AssertExpectations(t)

		pluginRepo := new(mock.SupportedPluginRepo)
		defer pluginRepo.AssertExpectations(t)
		pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
		adapter := NewAdapter(pluginRepo)

		var testModels []*models.ReplaySpec
		testModels = append(testModels, testConfigs...)

		jobConfigs[0].Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1}}
		testConfigs[0].Job = jobConfigs[0]

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err := jobRepo.Insert(ctx, jobConfigs[0])
		assert.Nil(t, err)

		repo := NewReplayRepository(db, adapter)
		err = repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].ID, checkModel.ID)
		assert.True(t, treeIsEqual(testModels[0].ExecutionTree, checkModel.ExecutionTree))
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		var testModels []*models.ReplaySpec
		testModels = append(testModels, testConfigs...)

		execUnit1 := new(mock.BasePlugin)
		defer execUnit1.AssertExpectations(t)
		execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: gTask,
		}, nil)
		depMod1 := new(mock.DependencyResolverMod)
		defer depMod1.AssertExpectations(t)

		pluginRepo := new(mock.SupportedPluginRepo)
		defer pluginRepo.AssertExpectations(t)
		pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
		adapter := NewAdapter(pluginRepo)

		jobConfigs[0].Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1}}
		testConfigs[0].Job = jobConfigs[0]

		projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err := jobRepo.Insert(ctx, jobConfigs[0])
		assert.Nil(t, err)

		repo := NewReplayRepository(db, adapter)
		err = repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		errMessage := "failed to execute"
		replayMessage := models.ReplayMessage{
			Type:    "test failure",
			Message: errMessage,
		}
		err = repo.UpdateStatus(ctx, testModels[0].ID, models.ReplayStatusFailed, replayMessage)
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, models.ReplayStatusFailed, checkModel.Status)
		assert.Equal(t, errMessage, checkModel.Message.Message)
	})

	t.Run("GetByStatus", func(t *testing.T) {
		t.Run("should return list of job specs given list of status", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			defer depMod1.AssertExpectations(t)

			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := NewAdapter(pluginRepo)

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err := jobRepo.Insert(ctx, testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			statusList := []string{models.ReplayStatusAccepted, models.ReplayStatusInProgress}
			replays, err := repo.GetByStatus(ctx, statusList)
			assert.Nil(t, err)
			assert.Equal(t, jobConfigs[0].ID, replays[0].Job.ID)
			assert.Equal(t, jobConfigs[2].ID, replays[1].Job.ID)
		})
	})

	t.Run("GetByJobIDAndStatus", func(t *testing.T) {
		t.Run("should return list of replay specs given job_id and list of status", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			defer depMod1.AssertExpectations(t)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := NewAdapter(pluginRepo)

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err := jobRepo.Insert(ctx, testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			statusList := []string{models.ReplayStatusAccepted, models.ReplayStatusInProgress}
			replays, err := repo.GetByJobIDAndStatus(ctx, testModels[2].Job.ID, statusList)
			assert.Nil(t, err)
			assert.Equal(t, jobConfigs[2].ID, replays[0].Job.ID)
		})
	})
	t.Run("GetByProjectIDAndStatus", func(t *testing.T) {
		t.Run("should return list of replay specs given project_id and list of status", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			defer depMod1.AssertExpectations(t)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := NewAdapter(pluginRepo)

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			projectRepo := NewProjectRepository(db, hash)

			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			statusList := []string{models.ReplayStatusAccepted, models.ReplayStatusInProgress}
			replays, err := repo.GetByProjectIDAndStatus(ctx, projectSpec.ID, statusList)
			assert.Nil(t, err)
			assert.ElementsMatch(t, []uuid.UUID{testModels[0].ID, testModels[2].ID}, []uuid.UUID{replays[0].ID, replays[1].ID})
		})
	})
	t.Run("GetByProjectID", func(t *testing.T) {
		t.Run("should return list of replay specs given project_id", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)
			expectedUUIDs := []uuid.UUID{testModels[0].ID, testModels[1].ID, testModels[2].ID}

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			defer depMod1.AssertExpectations(t)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := NewAdapter(pluginRepo)

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			projectRepo := NewProjectRepository(db, hash)

			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			replays, err := repo.GetByProjectID(ctx, projectSpec.ID)
			assert.Nil(t, err)
			assert.ElementsMatch(t, expectedUUIDs, []uuid.UUID{replays[0].ID, replays[1].ID, replays[2].ID})
		})
		t.Run("should return not found if no recent replay is found", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			defer depMod1.AssertExpectations(t)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			adapter := NewAdapter(pluginRepo)

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
			}
			depMod1.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

			projectJobSpecRepo := NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			projectRepo := NewProjectRepository(db, hash)

			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			replays, err := repo.GetByProjectID(ctx, uuid.Must(uuid.NewRandom()))
			assert.Equal(t, store.ErrResourceNotFound, err)
			assert.Equal(t, []models.ReplaySpec{}, replays)
		})
	})
}
