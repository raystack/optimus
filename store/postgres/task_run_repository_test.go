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

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
)

func TestIntegrationTaskRunRepository(t *testing.T) {
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
		prepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, prepo.Save(ctx, projectSpec))

		projectJobSpecRepo := postgres.NewProjectJobSpecRepository(dbConn, projectSpec, adapter)
		jrepo := postgres.NewNamespaceJobSpecRepository(dbConn, namespaceSpec, projectJobSpecRepo, adapter)
		assert.Nil(t, jrepo.Save(ctx, jobConfigs[0], jobDestination))
		assert.Equal(t, "task unit cannot be empty", jrepo.Save(ctx, jobConfigs[1], jobDestination).Error())
		return dbConn
	}
	//
	//scheduledAt, _ := time.Parse(time.RFC3339, "2022-01-02T20:34:05+05:30")
	//startTime, _ := time.Parse(time.RFC3339, "2022-01-01T05:30:00+05:30")
	//endTime, _ := time.Parse(time.RFC3339, "3000-09-17T00:47:23+05:30")

	SLAMissDuearionSecs := int64(100)
	jobStartEventTimeString := "2022-01-02T16:04:05Z"
	jobStartEventTime, _ := time.Parse(time.RFC3339, jobStartEventTimeString)
	taskEventTimeString := jobStartEventTimeString
	scheduledAt := "2022-01-02T15:04:05Z"
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"attempt":      "2",
			"event_time":   jobStartEventTime.Unix(),
		},
	)
	taskStartTime, _ := time.Parse(time.RFC3339, taskEventTimeString)
	taskEventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"attempt":      "2",
			"event_time":   taskStartTime.Unix(),
		},
	)

	jobEvent := models.JobEvent{
		Type:  models.JobStartEvent,
		Value: eventValues.GetFields(),
	}

	taskRunStartEvent := models.JobEvent{
		Type:  models.TaskStartEvent,
		Value: taskEventValues.GetFields(),
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
		err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
		assert.Nil(t, err)
		jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
		assert.Nil(t, err)

		repo := postgres.NewTaskRunRepository(db)
		err = repo.Save(ctx, taskRunStartEvent, jobRunSpec)
		assert.Nil(t, err)

		taskRunSpec, err := repo.GetTaskRun(ctx, jobRunSpec)
		assert.Nil(t, err)

		assert.Equal(t, taskRunSpec.JobRunID, jobRunSpec.JobRunID)
		assert.Equal(t, taskRunSpec.StartTime.UTC(), taskStartTime.UTC())
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update task runs correctly", func(t *testing.T) {
			db := DBSetup()
			jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
			err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)
			jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			repo := postgres.NewTaskRunRepository(db)
			err = repo.Save(ctx, taskRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			taskEndEventTimeString := "2022-01-02T17:04:05Z"
			taskEndTime, err := time.Parse(time.RFC3339, taskEndEventTimeString)
			assert.Nil(t, err)

			updateEventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   taskEndTime.Unix(),
					"attempt":      "2",
					"scheduled_at": scheduledAt,
					"status":       "SUCCESS",
				},
			)
			jobUpdateEvent := models.JobEvent{
				Type:  models.TaskSuccessEvent,
				Value: updateEventValues.GetFields(),
			}

			err = repo.Update(ctx, jobUpdateEvent, jobRunSpec)
			assert.Nil(t, err)

			taskRunSpec, err := repo.GetTaskRun(ctx, jobRunSpec)
			assert.Nil(t, err)

			assert.Equal(t, taskRunSpec.JobRunID, jobRunSpec.JobRunID)
			assert.Equal(t, taskRunSpec.Duration, taskEndTime.Unix()-taskStartTime.Unix())
			assert.Equal(t, "SUCCESS", taskRunSpec.Status)
		})
	})
	t.Run("GetTaskRun", func(t *testing.T) {
		t.Run("should return latest task run attempt for a given jobRun", func(t *testing.T) {
			db := DBSetup()
			jobUpdateEventAttempt3Time, err := time.Parse(time.RFC3339, "2022-01-02T18:04:05Z")
			assert.Nil(t, err)

			eventValuesAttempt3, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"attempt":      3,
					"event_time":   jobUpdateEventAttempt3Time.Unix(),
				},
			)
			jobUpdateEventAttempt3 := models.JobEvent{
				Type:  models.JobStartEvent,
				Value: eventValuesAttempt3.GetFields(),
			}

			jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
			// adding for attempt number 2
			err = jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			jobRunSpec, err := jobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, jobUpdateEventAttempt3.Value["scheduled_at"].GetStringValue(), namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			// first task run for attempt number 2
			repo := postgres.NewTaskRunRepository(db)
			err = repo.Save(ctx, taskRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			// adding for attempt number 3
			err = jobRunMetricsRepository.Save(ctx, jobUpdateEventAttempt3, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			JobSuccessEventTime, err := time.Parse(time.RFC3339, "2022-01-02T15:04:05Z")
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

			// first task run for attempt number 3
			err = repo.Save(ctx, taskRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			taskRunSpec, err := repo.GetTaskRun(ctx, jobRunSpec)
			assert.Equal(t, taskRunSpec.JobRunAttempt, jobRunSpec.Attempt)
			assert.Nil(t, err)
			assert.Equal(t, taskRunSpec.JobRunID, jobRunSpec.JobRunID)
			assert.Nil(t, err)
		})
	})
}
