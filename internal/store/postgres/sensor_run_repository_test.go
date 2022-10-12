//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestIntegrationSensorRunRepository(t *testing.T) {
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

	pluginRepo := mock.NewPluginRepository(t)
	adapter := postgres.NewAdapter(pluginRepo)

	window, err := models.NewWindow(1, "", "", "")
	if err != nil {
		panic(err)
	}

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
				Window: window,
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
			ResourceDestination: jobDestination,
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
		return dbConn
	}

	SLAMissDuearionSecs := int64(100)
	jobStartEventTimeString := "2022-01-02T16:04:05Z"
	jobStartEventTime, _ := time.Parse(time.RFC3339, jobStartEventTimeString)
	sensorEventTimeString := jobStartEventTimeString
	scheduledAt := "2022-01-02T15:04:05Z"
	jobStartEventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"attempt":      "2",
			"event_time":   jobStartEventTime.Unix(),
		},
	)
	sensorStartTime, _ := time.Parse(time.RFC3339, sensorEventTimeString)
	sensorEventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"attempt":      "2",
			"event_time":   sensorStartTime.Unix(),
		},
	)

	jobEvent := models.JobEvent{
		Type:  models.JobStartEvent,
		Value: jobStartEventValues.GetFields(),
	}

	sensorRunStartEvent := models.JobEvent{
		Type:  models.SensorStartEvent,
		Value: sensorEventValues.GetFields(),
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
		err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
		assert.Nil(t, err)
		jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
		assert.Nil(t, err)

		repo := postgres.NewSensorRunRepository(db)
		err = repo.Save(ctx, sensorRunStartEvent, jobRunSpec)
		assert.Nil(t, err)

		sensorRunSpec, err := repo.GetSensorRun(ctx, jobRunSpec)
		assert.Nil(t, err)

		assert.Equal(t, sensorRunSpec.JobRunID, jobRunSpec.JobRunID)
		assert.Equal(t, sensorRunSpec.StartTime.UTC(), sensorStartTime.UTC())
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update sensor runs correctly", func(t *testing.T) {
			db := DBSetup()
			jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
			err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)
			jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			repo := postgres.NewSensorRunRepository(db)
			err = repo.Save(ctx, sensorRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			sensorEndEventTimeString := "2022-01-02T17:04:05Z"
			sensorEndTime, err := time.Parse(time.RFC3339, sensorEndEventTimeString)
			assert.Nil(t, err)

			updateEventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   sensorEndTime.Unix(),
					"scheduled_at": scheduledAt,
					"attempt":      "2",
					"status":       "SUCCESS",
				},
			)
			jobUpdateEvent := models.JobEvent{
				Type:  models.SensorSuccessEvent,
				Value: updateEventValues.GetFields(),
			}

			err = repo.Update(ctx, jobUpdateEvent, jobRunSpec)
			assert.Nil(t, err)

			sensorRunSpec, err := repo.GetSensorRun(ctx, jobRunSpec)
			assert.Nil(t, err)

			assert.Equal(t, jobRunSpec.JobRunID, sensorRunSpec.JobRunID)
			assert.Equal(t, sensorEndTime.Unix()-sensorStartTime.Unix(), sensorRunSpec.Duration)
			assert.Equal(t, "SUCCESS", sensorRunSpec.Status)
		})
	})
	t.Run("GetSensorRun", func(t *testing.T) {
		t.Run("should return latest sensor run attempt for a given jobRun", func(t *testing.T) {
			db := DBSetup()
			JobStartEventTime, _ := time.Parse(time.RFC3339, "2022-01-02T18:04:05Z")
			eventValuesAttempt3, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"attempt":      3,
					"event_time":   JobStartEventTime.Unix(),
				},
			)
			jobUpdateEventAttempt3 := models.JobEvent{
				Type:  models.JobStartEvent,
				Value: eventValuesAttempt3.GetFields(),
			}

			jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
			// adding for attempt number 2
			err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			jobRunSpec, err := jobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, jobUpdateEventAttempt3.Value["scheduled_at"].GetStringValue(), namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			// first sensor run for attempt number 2
			repo := postgres.NewSensorRunRepository(db)
			err = repo.Save(ctx, sensorRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			// adding for attempt number 3
			err = jobRunMetricsRepository.Save(ctx, jobUpdateEventAttempt3, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			JobSuccessEventTime, err := time.Parse(time.RFC3339, "2022-01-02T18:14:05Z")
			assert.Nil(t, err)
			eventValuesAttemptFinish, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"attempt":      "3",
					"event_time":   JobSuccessEventTime.Unix(),
				},
			)
			jobSuccessEventAttempt3 := models.JobEvent{
				Type:  models.JobSuccessEvent,
				Value: eventValuesAttemptFinish.GetFields(),
			}
			// should return the latest attempt number
			jobRunSpec, err = jobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, jobSuccessEventAttempt3.Value["scheduled_at"].GetStringValue(), namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			// first sensor run for attempt number 3
			err = repo.Save(ctx, sensorRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			sensorRunSpec, err := repo.GetSensorRun(ctx, jobRunSpec)
			assert.Equal(t, sensorRunSpec.JobRunAttempt, jobRunSpec.Attempt)
			assert.Nil(t, err)
		})
	})
}
