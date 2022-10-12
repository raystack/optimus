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

	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestIntegrationInstanceRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}
	ctx := context.Background()

	jobDestination := "project.dataset.table"

	gTask := "g-task"
	tTask := "t-task"
	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: gTask,
	}, nil)
	execUnit2 := new(mock.BasePlugin)
	execUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: tTask,
	}, nil)

	pluginRepo := mock.NewPluginRepository(t)
	adapter := postgres.NewAdapter(pluginRepo)

	jobConfigs := []models.JobSpec{
		{
			ID:   uuid.New(),
			Name: "g-optimus-id",
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit1},
				Config: []models.JobSpecConfigItem{
					{
						Name: "do", Value: "this",
					},
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
			ResourceDestination: jobDestination,
		},
		{
			Name:                "",
			ResourceDestination: jobDestination,
		},
		{
			ID:   uuid.New(),
			Name: "t-optimus-id",
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit2},
				Config: []models.JobSpecConfigItem{
					{
						Name: "do", Value: "this",
					},
				},
			},
		},
	}

	jobRuns := []models.JobRun{
		{
			ID:          uuid.New(),
			Spec:        jobConfigs[0],
			Trigger:     models.TriggerSchedule,
			Status:      models.RunStateRunning,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
		},
	}

	unitData := models.GenerateDestinationRequest{
		Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
		Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
	}
	execUnit1.On("GenerateDestination", context.Background(), unitData).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)
	unitData2 := models.GenerateDestinationRequest{
		Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[1].Task.Config),
		Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[1].Assets),
	}
	execUnit2.On("GenerateDestination", context.Background(), unitData2).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)
		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

		projectRepository := postgres.NewProjectRepository(dbConn, hash)
		err := projectRepository.Save(ctx, projectSpec)
		assert.NoError(t, err)

		storedProjects := readStoredRecordsByFilter[*postgres.Project](dbConn, map[string]interface{}{
			"name": projectSpec.Name,
		})
		projectSpec.ID = models.ProjectID(storedProjects[0].ID)

		namespaceRepository := postgres.NewNamespaceRepository(dbConn, hash)
		err = namespaceRepository.Save(ctx, projectSpec, namespaceSpec)
		assert.NoError(t, err)

		storedNamespaces := readStoredRecordsByFilter[*postgres.Namespace](dbConn, map[string]interface{}{
			"project_id": projectSpec.ID.UUID(),
			"name":       namespaceSpec.Name,
		})
		namespaceSpec.ID = storedNamespaces[0].ID

		for i := 0; i < len(jobConfigs); i++ {
			jobConfigs[i].NamespaceSpec = namespaceSpec
		}

		jobSpecRepository, err := postgres.NewJobSpecRepository(dbConn, adapter)
		assert.NoError(t, err)
		err = jobSpecRepository.Save(ctx, jobConfigs[0])
		assert.NoError(t, err)
		err = jobSpecRepository.Save(ctx, jobConfigs[1])
		assert.EqualError(t, err, "task unit cannot be empty")

		jobRunRepo := postgres.NewJobRunRepository(dbConn, adapter)
		err = jobRunRepo.Save(ctx, namespaceSpec, jobRuns[0], jobDestination)
		assert.Nil(t, err)
		return dbConn
	}

	testSpecs := []models.InstanceSpec{
		{
			ID:         uuid.New(),
			Name:       gTask,
			Type:       models.InstanceTypeTask,
			Status:     models.RunStateSuccess,
			ExecutedAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			Data: []models.JobRunSpecData{
				{Name: "dstart", Value: "2020-01-02", Type: models.InstanceDataTypeEnv},
			},
		},
		{
			ID: uuid.New(),
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.InstanceSpec
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewInstanceRepository(db, adapter)
		err := repo.Insert(ctx, jobRuns[0], testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Name, checkModel.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)
	})
	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewInstanceRepository(db, adapter)
		err := repo.Insert(ctx, jobRuns[0], testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Name, checkModel.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)

		err = repo.Delete(ctx, testModels[0].ID)
		assert.Nil(t, err)

		testModels[0].Name = "updated-name"

		err = repo.Save(ctx, jobRuns[0], testModels[0])
		assert.Nil(t, err)

		checkModel, err = repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, "updated-name", checkModel.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)
	})
	t.Run("UpdateStatus", func(t *testing.T) {
		db := DBSetup()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewInstanceRepository(db, adapter)
		err := repo.Save(ctx, jobRuns[0], testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Name, checkModel.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)

		err = repo.UpdateStatus(ctx, testModels[0].ID, models.RunStateFailed)
		assert.Nil(t, err)

		checkModel, err = repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, models.RunStateFailed, checkModel.Status)
	})
}
