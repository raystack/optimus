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

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

func treeIsEqual(treeNode, treeNodeComparator *tree.TreeNode) bool {
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
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		Secret: []models.ProjectSecretItem{
			{
				ID:    uuid.New(),
				Name:  "k1",
				Value: "v1",
			},
		},
	}
	jobDestination := "project.dataset.table"
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	gTask := "g-task"
	jobConfigs := []models.JobSpec{
		{
			ID:            uuid.New(),
			Name:          "job-1",
			NamespaceSpec: namespaceSpec,
		},
		{
			ID:            uuid.New(),
			Name:          "job-2",
			NamespaceSpec: namespaceSpec,
		},
		{
			ID:            uuid.New(),
			Name:          "job-3",
			NamespaceSpec: namespaceSpec,
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
			ID:            uuid.New(),
			StartDate:     startTime,
			EndDate:       endTime,
			Status:        models.ReplayStatusAccepted,
			ExecutionTree: treeNode1,
			Config:        map[string]string{models.ConfigIgnoreDownstream: "true"},
		},
		{
			ID:        uuid.New(),
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusFailed,
		},
		{
			ID:        uuid.New(),
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusInProgress,
		},
	}

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		return dbConn
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	t.Run("Insert and GetByID", func(t *testing.T) {
		db := DBSetup()

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
		adapter := postgres.NewAdapter(pluginRepo)

		var testModels []*models.ReplaySpec
		testModels = append(testModels, testConfigs...)

		jobConfigs[0].Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1}}
		testConfigs[0].Job = jobConfigs[0]

		namespaceRepo := postgres.NewNamespaceRepository(db, hash)
		err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
		assert.Nil(t, err)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

		err = jobRepo.Insert(ctx, jobConfigs[0], jobDestination)
		assert.Nil(t, err)

		repo := postgres.NewReplayRepository(db, adapter)
		err = repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].ID, checkModel.ID)
		assert.True(t, treeIsEqual(testModels[0].ExecutionTree, checkModel.ExecutionTree))
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		db := DBSetup()

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
		adapter := postgres.NewAdapter(pluginRepo)

		jobConfigs[0].Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1}}
		testConfigs[0].Job = jobConfigs[0]

		namespaceRepo := postgres.NewNamespaceRepository(db, hash)
		err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
		assert.Nil(t, err)

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
		jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
		err = jobRepo.Insert(ctx, jobConfigs[0], jobDestination)
		assert.Nil(t, err)

		repo := postgres.NewReplayRepository(db, adapter)
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

			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)

			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := postgres.NewAdapter(pluginRepo)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err = jobRepo.Insert(ctx, testModels[0].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job, jobDestination)
			assert.Nil(t, err)

			repo := postgres.NewReplayRepository(db, adapter)
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

			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := postgres.NewAdapter(pluginRepo)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err := namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)
			err = jobRepo.Insert(ctx, testModels[0].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job, jobDestination)
			assert.Nil(t, err)

			repo := postgres.NewReplayRepository(db, adapter)
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

			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)

			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := postgres.NewAdapter(pluginRepo)

			projectRepo := postgres.NewProjectRepository(db, hash)
			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err = namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err = jobRepo.Insert(ctx, testModels[0].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job, jobDestination)
			assert.Nil(t, err)

			repo := postgres.NewReplayRepository(db, adapter)
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

			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)
			expectedUUIDs := []uuid.UUID{testModels[0].ID, testModels[1].ID, testModels[2].ID}

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1, DependencyMod: depMod1}, nil)
			adapter := postgres.NewAdapter(pluginRepo)

			projectRepo := postgres.NewProjectRepository(db, hash)
			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err = namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err = jobRepo.Insert(ctx, testModels[0].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job, jobDestination)
			assert.Nil(t, err)

			repo := postgres.NewReplayRepository(db, adapter)
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

			var testModels []*models.ReplaySpec
			testModels = append(testModels, testConfigs...)

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: gTask,
			}, nil)
			depMod1 := new(mock.DependencyResolverMod)
			for idx, jobConfig := range jobConfigs {
				jobConfig.Task = models.JobSpecTask{Unit: &models.Plugin{Base: execUnit1, DependencyMod: depMod1}}
				testConfigs[idx].Job = jobConfig
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			defer pluginRepo.AssertExpectations(t)
			adapter := postgres.NewAdapter(pluginRepo)

			projectRepo := postgres.NewProjectRepository(db, hash)
			err := projectRepo.Insert(ctx, projectSpec)
			assert.Nil(t, err)

			namespaceRepo := postgres.NewNamespaceRepository(db, hash)
			err = namespaceRepo.Insert(ctx, projectSpec, namespaceSpec)
			assert.Nil(t, err)

			projectJobSpecRepo := postgres.NewProjectJobSpecRepository(db, projectSpec, adapter)
			jobRepo := postgres.NewJobSpecRepository(db, namespaceSpec, projectJobSpecRepo, adapter)

			err = jobRepo.Insert(ctx, testModels[0].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[1].Job, jobDestination)
			assert.Nil(t, err)
			err = jobRepo.Insert(ctx, testModels[2].Job, jobDestination)
			assert.Nil(t, err)

			repo := postgres.NewReplayRepository(db, adapter)
			err = repo.Insert(ctx, testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(ctx, testModels[2])
			assert.Nil(t, err)

			replays, err := repo.GetByProjectID(ctx, models.ProjectID(uuid.New()))
			assert.Equal(t, store.ErrResourceNotFound, err)
			assert.Equal(t, []models.ReplaySpec{}, replays)
		})
	})
}
