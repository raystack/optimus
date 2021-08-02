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

func TestReplayRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
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
			ID:      uuid.Must(uuid.NewRandom()),
			Name:    "job-1",
			Project: projectSpec,
		},
		{
			ID:      uuid.Must(uuid.NewRandom()),
			Name:    "job-2",
			Project: projectSpec,
		},
		{
			ID:      uuid.Must(uuid.NewRandom()),
			Name:    "job-3",
			Project: projectSpec,
		},
	}
	startTime := time.Date(2021, 1, 15, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2021, 1, 20, 0, 0, 0, 0, time.UTC)
	testConfigs := []*models.ReplaySpec{
		{
			ID:        uuid.Must(uuid.NewRandom()),
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusAccepted,
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
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	t.Run("Insert and GetByID", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

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
		err := jobRepo.Insert(jobConfigs[0])
		assert.Nil(t, err)

		repo := NewReplayRepository(db, jobConfigs[0], adapter)
		err = repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].ID, checkModel.ID)
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
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
		err := jobRepo.Insert(jobConfigs[0])
		assert.Nil(t, err)

		repo := NewReplayRepository(db, jobConfigs[0], adapter)
		err = repo.Insert(testModels[0])
		assert.Nil(t, err)

		errMessage := "failed to execute"
		replayMessage := models.ReplayMessage{
			Type:    "test failure",
			Message: errMessage,
		}
		err = repo.UpdateStatus(testModels[0].ID, models.ReplayStatusFailed, replayMessage)
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, models.ReplayStatusFailed, checkModel.Status)
		assert.Equal(t, errMessage, checkModel.Message.Message)
	})

	t.Run("GetJobByStatus", func(t *testing.T) {
		t.Run("should return list of job specs given list of status", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
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

			err := projectRepo.Insert(projectSpec)
			assert.Nil(t, err)

			err = jobRepo.Insert(testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, jobConfigs[0], adapter)
			err = repo.Insert(testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(testModels[2])
			assert.Nil(t, err)

			statusList := []string{models.ReplayStatusAccepted, models.ReplayStatusInProgress}
			replays, err := repo.GetByStatus(statusList)
			assert.Nil(t, err)
			assert.Equal(t, jobConfigs[0].ID, replays[0].Job.ID)
			assert.Equal(t, jobConfigs[2].ID, replays[1].Job.ID)
			assert.Equal(t, jobConfigs[0].Project.Name, replays[0].Job.Project.Name)
		})
	})

	t.Run("GetJobByIDAndStatus", func(t *testing.T) {
		t.Run("should return list of job specs given job_id and list of status", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
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
			err := jobRepo.Insert(testModels[0].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(testModels[1].Job)
			assert.Nil(t, err)
			err = jobRepo.Insert(testModels[2].Job)
			assert.Nil(t, err)

			repo := NewReplayRepository(db, jobConfigs[0], adapter)
			err = repo.Insert(testModels[0])
			assert.Nil(t, err)
			err = repo.Insert(testModels[1])
			assert.Nil(t, err)
			err = repo.Insert(testModels[2])
			assert.Nil(t, err)

			statusList := []string{models.ReplayStatusAccepted, models.ReplayStatusInProgress}
			replays, err := repo.GetByJobIDAndStatus(testModels[2].Job.ID, statusList)
			assert.Nil(t, err)
			assert.Equal(t, jobConfigs[2].ID, replays[0].Job.ID)
		})
	})
}
