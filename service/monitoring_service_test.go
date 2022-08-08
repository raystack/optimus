package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

func TestMonitoringService(t *testing.T) {
	ctx := context.Background()
	depMod := new(mock.DependencyResolverMod)
	baseUnit := new(mock.BasePlugin)
	plugin := &models.Plugin{Base: baseUnit, DependencyMod: depMod}
	baseUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: "bq",
	}, nil)

	projectName := "a-data-project"
	projectSpec := models.ProjectSpec{
		Name: projectName,
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		Name: "namespace-123",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: projectSpec,
	}
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url":          "https://example.io",
			"scheduled_at": "2022-01-02T15:04:05Z",
			"attempt":      "2",
			"event_time":   "2022-01-02T16:04:05Z",
		},
	)
	jobDestination := "p.d.t"
	jobSpec := models.JobSpec{
		Version: 1,
		Name:    "test1",
		Task: models.JobSpecTask{
			Unit: plugin,
			Config: models.JobSpecConfigs{
				{
					Name:  "foo",
					Value: "bar",
				},
			},
		},
		ResourceDestination: jobDestination,
	}
	jobRunMetricsRepository := new(mock.JobRunMetricsRepository)

	taskRunRepository := new(mock.TaskRunRepository)

	sensorRunRepository := new(mock.SensorRunRepository)

	hookRunRepository := new(mock.HookRunRepository)

	monitoringService := service.NewMonitoringService(
		jobRunMetricsRepository,
		sensorRunRepository,
		hookRunRepository,
		taskRunRepository)

	t.Run("ProcessEvent", func(t *testing.T) {
		t.Run("on JobStartEvent, successfully registerNewJobRun", func(t *testing.T) {
			jobStartEvent := models.JobEvent{
				Type:  models.JobStartEvent,
				Value: eventValues.GetFields(),
			}
			slaMissDurationInSec, err := jobSpec.SLADuration()
			assert.Nil(t, err)

			jobRunMetricsRepository.On(
				"Save",
				ctx,
				jobStartEvent,
				namespaceSpec,
				jobSpec,
				slaMissDurationInSec,
			).Return(nil)
			defer jobRunMetricsRepository.AssertExpectations(t)

			err = monitoringService.ProcessEvent(ctx, jobStartEvent, namespaceSpec, jobSpec)
			assert.Nil(t, err)
		})
		t.Run("on JobSuccessEvent, successfully updateJobRun", func(t *testing.T) {
			jobStartEvent := models.JobEvent{
				Type:  models.JobSuccessEvent,
				Value: eventValues.GetFields(),
			}

			jobRunMetricsRepository.On(
				"Update",
				ctx,
				jobStartEvent,
				namespaceSpec,
				jobSpec,
			).Return(nil)
			defer jobRunMetricsRepository.AssertExpectations(t)
			err := monitoringService.ProcessEvent(ctx, jobStartEvent, namespaceSpec, jobSpec)
			assert.Nil(t, err)
		})
		t.Run("on TaskEvents, successfully registerTaskRunEvent", func(t *testing.T) {
			t.Run(" if TaskRun does not exists then create a new task", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.TaskStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}

				taskRunRepository.On(
					"GetTaskRun",
					ctx,
					jobRunSpec,
				).Return(models.TaskRunSpec{}, store.ErrResourceNotFound).Once()

				taskRunRepository.On(
					"Save",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer taskRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
			t.Run(" if TaskRun exists then update the task", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.TaskStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}
				taskRunRepository.On(
					"GetTaskRun",
					ctx,
					jobRunSpec,
				).Return(models.TaskRunSpec{}, nil).Once()

				taskRunRepository.On(
					"Update",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer taskRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
		})
		t.Run("on SensorEvents, successfully registerSensorRunEvent", func(t *testing.T) {
			t.Run(" if SensorRun does not exists then create a new sensor", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.SensorStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}

				sensorRunRepository.On(
					"GetSensorRun",
					ctx,
					jobRunSpec,
				).Return(models.SensorRunSpec{}, store.ErrResourceNotFound).Once()

				sensorRunRepository.On(
					"Save",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer sensorRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
			t.Run(" if SensorRun exists then update the sensor", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.SensorStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}
				sensorRunRepository.On(
					"GetSensorRun",
					ctx,
					jobRunSpec,
				).Return(models.SensorRunSpec{}, nil).Once()

				sensorRunRepository.On(
					"Update",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer sensorRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
		})
		t.Run("on HookEvents, successfully registerHookRunEvent", func(t *testing.T) {
			t.Run(" if HookRun does not exists then create a new hook", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.HookStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}

				hookRunRepository.On(
					"GetHookRun",
					ctx,
					jobRunSpec,
				).Return(models.HookRunSpec{}, store.ErrResourceNotFound).Once()

				hookRunRepository.On(
					"Save",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer hookRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
			t.Run(" if HookRun exists then update the hook", func(t *testing.T) {
				event := models.JobEvent{
					Type:  models.HookStartEvent,
					Value: eventValues.GetFields(),
				}
				jobRunSpec := models.JobRunSpec{}
				hookRunRepository.On(
					"GetHookRun",
					ctx,
					jobRunSpec,
				).Return(models.HookRunSpec{}, nil).Once()

				hookRunRepository.On(
					"Update",
					ctx,
					event,
					jobRunSpec,
				).Return(nil)
				defer hookRunRepository.AssertExpectations(t)

				jobRunMetricsRepository.On(
					"GetLatestJobRunByScheduledTime",
					ctx,
					event.Value["scheduled_at"].GetStringValue(),
					namespaceSpec,
					jobSpec,
				).Return(jobRunSpec, nil)
				defer jobRunMetricsRepository.AssertExpectations(t)

				err := monitoringService.ProcessEvent(ctx, event, namespaceSpec, jobSpec)
				assert.Nil(t, err)
			})
		})
	})
}
