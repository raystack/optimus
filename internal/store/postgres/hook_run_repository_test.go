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

func TestIntegrationHookRunRepository(t *testing.T) {
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
	//
	//scheduledAt, _ := time.Parse(time.RFC3339, "2022-01-02T20:34:05+05:30")
	//startTime, _ := time.Parse(time.RFC3339, "2022-01-01T05:30:00+05:30")
	//endTime, _ := time.Parse(time.RFC3339, "3000-09-17T00:47:23+05:30")

	SLAMissDuearionSecs := int64(100)
	eventTimeString := "2022-01-02T16:04:05Z"
	hookEventTimeString := "2022-01-02T16:04:05Z"
	scheduledAt := "2022-01-02T15:04:05Z"
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"attempt":      "2",
			"event_time":   eventTimeString,
		},
	)
	hookStartTime, _ := time.Parse(time.RFC3339, hookEventTimeString)
	hookEventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": scheduledAt,
			"event_time":   hookStartTime.Unix(),
			"attempt":      "2",
		},
	)

	jobEvent := models.JobEvent{
		Type:  models.JobStartEvent,
		Value: eventValues.GetFields(),
	}

	hookRunStartEvent := models.JobEvent{
		Type:  models.HookStartEvent,
		Value: hookEventValues.GetFields(),
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
		err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
		assert.Nil(t, err)
		jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
		assert.Nil(t, err)
		repo := postgres.NewHookRunRepository(db)
		err = repo.Save(ctx, hookRunStartEvent, jobRunSpec)
		assert.Nil(t, err)

		hookRunSpec, err := repo.GetHookRun(ctx, jobRunSpec)
		assert.Nil(t, err)

		assert.Equal(t, hookRunSpec.JobRunID, jobRunSpec.JobRunID)
		assert.Equal(t, hookRunSpec.StartTime.UTC(), hookStartTime.UTC())
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update hook runs correctly", func(t *testing.T) {
			db := DBSetup()
			jobRunMetricsRepository := postgres.NewJobRunMetricsRepository(db)
			err := jobRunMetricsRepository.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			jobRunSpec, err := jobRunMetricsRepository.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			repo := postgres.NewHookRunRepository(db)
			err = repo.Save(ctx, hookRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			hookEndEventTimeString := "2022-01-02T17:04:05Z"
			hookEndTime, err := time.Parse(time.RFC3339, hookEndEventTimeString)
			assert.Nil(t, err)
			updateEventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": scheduledAt,
					"event_time":   hookEndTime.Unix(),
					"attempt":      "2",
					"status":       "SUCCESS",
				},
			)
			jobUpdateEvent := models.JobEvent{
				Type:  models.HookSuccessEvent,
				Value: updateEventValues.GetFields(),
			}

			err = repo.Update(ctx, jobUpdateEvent, jobRunSpec)
			assert.Nil(t, err)

			hookRunSpec, err := repo.GetHookRun(ctx, jobRunSpec)
			assert.Nil(t, err)

			assert.Equal(t, hookRunSpec.JobRunID, jobRunSpec.JobRunID)
			assert.Equal(t, hookEndTime.Unix()-hookStartTime.Unix(), hookRunSpec.Duration)
			assert.Equal(t, "SUCCESS", hookRunSpec.Status)
		})
	})
	t.Run("GetHookRun", func(t *testing.T) {
		t.Run("should return latest hook run attempt for a given jobRun", func(t *testing.T) {
			db := DBSetup()

			eventValuesAttempt3, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"attempt":      3,
					"event_time":   "2022-01-02T18:04:05Z",
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
			// first hook run for attempt number 2
			repo := postgres.NewHookRunRepository(db)
			err = repo.Save(ctx, hookRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			// adding for attempt number 3
			err = jobRunMetricsRepository.Save(ctx, jobUpdateEventAttempt3, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			eventValuesAttemptFinish, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"attempt":      "3",
					"event_time":   "2022-01-02T28:04:05Z",
				},
			)
			jobSuccessEventAttempt3 := models.JobEvent{
				Type:  models.JobSuccessEvent,
				Value: eventValuesAttemptFinish.GetFields(),
			}
			// should return the latest attempt number
			jobRunSpec, err = jobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, jobSuccessEventAttempt3.Value["scheduled_at"].GetStringValue(), namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			// first hook run for attempt number 3
			err = repo.Save(ctx, hookRunStartEvent, jobRunSpec)
			assert.Nil(t, err)

			hookRunSpec, err := repo.GetHookRun(ctx, jobRunSpec)
			assert.Equal(t, hookRunSpec.JobRunAttempt, jobRunSpec.Attempt)
			assert.Nil(t, err)
		})
	})
}
