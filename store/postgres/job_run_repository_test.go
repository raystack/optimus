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

func TestIntegrationJobRunMetricsRepository(t *testing.T) {
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
				Window: models.WindowV1{},
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
		jrepo := postgres.NewNamespaceJobSpecRepository(dbConn, namespaceSpec, projectJobSpecRepo, adapter)
		assert.Nil(t, jrepo.Save(ctx, jobConfigs[0], jobDestination))
		assert.Equal(t, "task unit cannot be empty", jrepo.Save(ctx, jobConfigs[1], jobDestination).Error())
		return dbConn
	}

	SLAMissDuearionSecs := int64(100)
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": "2022-01-02T15:04:05Z",
			"attempt":      "2",
			"event_time":   "2022-01-02T16:04:05Z",
		},
	)
	jobEvent := models.JobEvent{
		Type:  models.JobStartEvent,
		Value: eventValues.GetFields(),
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		repo := postgres.NewJobRunMetricsRepository(db)
		err := repo.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
		assert.Nil(t, err)

		jobRunSpec, err := repo.Get(ctx, jobEvent, namespaceSpec, jobConfigs[0])
		assert.Nil(t, err)

		assert.Equal(t, jobRunSpec.JobID, jobConfigs[0].ID)
		assert.Equal(t, jobRunSpec.NamespaceID, namespaceSpec.ID)
		assert.Equal(t, jobRunSpec.Attempt, int(jobEvent.Value["attempt"].GetNumberValue()))
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update job runs correctly", func(t *testing.T) {
			db := DBSetup()

			repo := postgres.NewJobRunMetricsRepository(db)
			err := repo.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)

			updateEventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"event_time":   "2022-01-02T17:04:05Z",
					"attempt":      "2",
					"status":       "FINISHED",
				},
			)
			jobUpdateEvent := models.JobEvent{
				Type:  models.JobSuccessEvent,
				Value: updateEventValues.GetFields(),
			}

			err = repo.Update(ctx, jobUpdateEvent, namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			jobRunSpec, err := repo.Get(ctx, jobUpdateEvent, namespaceSpec, jobConfigs[0])
			assert.Nil(t, err)

			assert.Equal(t, jobConfigs[0].ID, jobRunSpec.JobID)
			assert.Equal(t, namespaceSpec.ID, jobRunSpec.NamespaceID)
			assert.Equal(t, int(jobEvent.Value["attempt"].GetNumberValue()), jobRunSpec.Attempt)
			assert.Equal(t, "FINISHED", jobRunSpec.Status)
			assert.Equal(t, int64(jobEvent.Value["attempt"].GetNumberValue()), jobRunSpec.Duration)
			assert.Equal(t, time.Unix(int64(jobEvent.Value["event_time"].GetNumberValue()), 0), jobRunSpec.EndTime)
		})
	})
	t.Run("GetLatestJobRunByScheduledTime", func(t *testing.T) {
		t.Run("should return latest job run attempt for a given scheduled time", func(t *testing.T) {
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

			repo := postgres.NewJobRunMetricsRepository(db)
			// adding for attempt number 2
			err := repo.Save(ctx, jobEvent, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
			assert.Nil(t, err)
			// adding for attempt number 3
			err = repo.Save(ctx, jobUpdateEventAttempt3, namespaceSpec, jobConfigs[0], SLAMissDuearionSecs)
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
			//should return the latest attempt number
			jobRunSpec, err := repo.GetLatestJobRunByScheduledTime(ctx, jobSuccessEventAttempt3.Value["scheduled_at"].GetStringValue(), namespaceSpec, jobConfigs[0])
			assert.Equal(t, jobRunSpec.Attempt, 3)
			assert.Nil(t, err)
		})
	})
}
