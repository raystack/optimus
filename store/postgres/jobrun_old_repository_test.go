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

func TestIntegrationJobRunRepository(t *testing.T) {
	ctx := context.Background()
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
	jobDestination := "p.d.t"
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

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", gTask).Return(&models.Plugin{Base: execUnit1}, nil)
	pluginRepo.On("GetByName", tTask).Return(&models.Plugin{Base: execUnit2}, nil)
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
		},
		{
			Name: "",
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

	unitData := models.GenerateDestinationRequest{
		Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[0].Task.Config),
		Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[0].Assets),
	}
	execUnit1.On("GenerateDestination", context.TODO(), unitData).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)
	unitData2 := models.GenerateDestinationRequest{
		Config: models.PluginConfigs{}.FromJobSpec(jobConfigs[1].Task.Config),
		Assets: models.PluginAssets{}.FromJobSpec(jobConfigs[1].Assets),
	}
	execUnit2.On("GenerateDestination", context.TODO(), unitData2).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		prepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, prepo.Save(ctx, projectSpec))

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(dbConn, projectSpec, adapter)

		taskRunRepository := postgres.NewTaskRunRepository(dbConn)
		sensorRunRepository := postgres.NewSensorRunRepository(dbConn)
		hookRunRepository := postgres.NewHookRunRepository(dbConn)
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(dbConn,
			*sensorRunRepository,
			*taskRunRepository,
			*hookRunRepository)

		jrepo := postgres.NewNamespaceJobSpecRepository(dbConn, namespaceSpec, projectJobSpecRepo, *jobRunMetricsRepository, adapter)
		assert.Nil(t, jrepo.Save(ctx, jobConfigs[0], jobDestination))
		assert.Equal(t, "task unit cannot be empty", jrepo.Save(ctx, jobConfigs[1], jobDestination).Error())
		return dbConn
	}

	testInstanceSpecs := []models.InstanceSpec{
		{
			ID:         uuid.New(),
			Name:       "do-this",
			Type:       models.InstanceTypeTask,
			ExecutedAt: time.Date(2020, 11, 11, 1, 0, 0, 0, time.UTC),
			Status:     models.RunStateSuccess,
			Data: []models.JobRunSpecData{
				{Name: "dstart", Value: "2020-01-02", Type: models.InstanceDataTypeEnv},
			},
		},
		{
			ID:   uuid.New(),
			Name: "do-that",
			Type: models.InstanceTypeTask,
		},
	}

	testSpecs := []models.JobRun{
		{
			ID:          uuid.New(),
			Spec:        jobConfigs[0],
			Trigger:     models.TriggerManual,
			Status:      models.RunStateRunning,
			Instances:   testInstanceSpecs,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:          uuid.New(),
			Spec:        jobConfigs[0],
			Trigger:     models.TriggerManual,
			Status:      models.RunStateRunning,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.JobRun
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewJobRunRepository(db, adapter)
		err := repo.Insert(ctx, namespaceSpec, testModels[1], jobDestination)
		assert.Nil(t, err)

		checkModel, ns, err := repo.GetByID(ctx, testModels[1].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[1].Spec.Name, checkModel.Spec.Name)
		assert.Equal(t, testModels[1].ScheduledAt.Unix(), checkModel.ScheduledAt.Unix())
		assert.Equal(t, namespaceSpec.ID, ns.ID)

		err = repo.Insert(ctx, namespaceSpec, testModels[0], jobDestination)
		assert.Nil(t, err)

		checkModel, ns, err = repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Spec.Name, checkModel.Spec.Name)
		assert.Equal(t, testModels[0].ScheduledAt.Unix(), checkModel.ScheduledAt.Unix())
		assert.Equal(t, namespaceSpec.ID, ns.ID)
		assert.Equal(t, 0, len(checkModel.Instances))
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("should save fresh runs correctly", func(t *testing.T) {
			db := DBSetup()

			testModels := []models.JobRun{}
			testModels = append(testModels, testSpecs...)

			repo := postgres.NewJobRunRepository(db, adapter)
			err := repo.Save(ctx, namespaceSpec, testModels[0], jobDestination)
			assert.Nil(t, err)

			checkModel, _, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, testModels[0].Spec.Name, checkModel.Spec.Name)
			assert.Equal(t, testModels[0].ScheduledAt.Unix(), checkModel.ScheduledAt.Unix())
		})
		t.Run("should upsert existing runs correctly", func(t *testing.T) {
			db := DBSetup()

			testModels := []models.JobRun{}
			testModels = append(testModels, testSpecs...)

			repo := postgres.NewJobRunRepository(db, adapter)
			err := repo.Save(ctx, namespaceSpec, testModels[0], jobDestination)
			assert.Nil(t, err)

			checkModel, _, err := repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, testModels[0].Spec.Name, checkModel.Spec.Name)
			assert.Equal(t, testModels[0].ScheduledAt.Unix(), checkModel.ScheduledAt.Unix())

			// update resource
			testModels[0].ScheduledAt = testModels[0].ScheduledAt.Add(time.Nanosecond)

			err = repo.Save(ctx, namespaceSpec, testModels[0], jobDestination)
			assert.Nil(t, err)

			checkModel, _, err = repo.GetByID(ctx, testModels[0].ID)
			assert.Nil(t, err)
			assert.Equal(t, testModels[0].Spec.Name, checkModel.Spec.Name)
			assert.Equal(t, testModels[0].ScheduledAt.Add(time.Nanosecond).Unix(), checkModel.ScheduledAt.Unix())
		})
	})
	t.Run("ClearInstance", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.JobRun
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewJobRunRepository(db, adapter)
		err := repo.Insert(ctx, namespaceSpec, testModels[0], jobDestination)
		assert.Nil(t, err)
		assert.Nil(t, repo.AddInstance(ctx, namespaceSpec, testModels[0], testModels[0].Instances[0]))
		assert.Nil(t, repo.AddInstance(ctx, namespaceSpec, testModels[0], testModels[0].Instances[1]))

		err = repo.ClearInstance(ctx, testModels[0].ID, models.InstanceTypeTask, "do-this")
		assert.Nil(t, err)

		checkModel, _, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(checkModel.Instances))
	})
	t.Run("AddInstance", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.JobRun
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewJobRunRepository(db, adapter)
		err := repo.Insert(ctx, namespaceSpec, testModels[1], jobDestination)
		assert.Nil(t, err)

		err = repo.AddInstance(ctx, namespaceSpec, testModels[1], testInstanceSpecs[0])
		assert.Nil(t, err)

		jr, _, err := repo.GetByID(ctx, testModels[1].ID)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(jr.Instances))
	})
	t.Run("Clear", func(t *testing.T) {
		db := DBSetup()

		var testModels []models.JobRun
		testModels = append(testModels, testSpecs...)

		repo := postgres.NewJobRunRepository(db, adapter)
		err := repo.Insert(ctx, namespaceSpec, testModels[0], jobDestination)
		assert.Nil(t, err)
		assert.Nil(t, repo.AddInstance(ctx, namespaceSpec, testModels[0], testModels[0].Instances[0]))
		assert.Nil(t, repo.AddInstance(ctx, namespaceSpec, testModels[0], testModels[0].Instances[1]))

		err = repo.Clear(ctx, testModels[0].ID)
		assert.Nil(t, err)

		jr, _, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(jr.Instances))
		assert.Equal(t, models.RunStatePending, jr.Status)
	})
}
