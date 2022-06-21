package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type monitoringService struct {
	logger        log.Logger
	JobRunService JobRunService

	JobRunMetricsRepository store.JobRunMetricsRepository
	TaskRunRepository       store.TaskRunRepository
	SensorRunRepository     store.SensorRunRepository
	HookRunRepository       store.HookRunRepository
}

type MonitoringService interface {
	ProcessEvent(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec) error
}

func getSLADuration(jobSpec models.JobSpec) (int64, error) {
	var slaMissDurationInSec int64
	for _, notify := range jobSpec.Behavior.Notify {
		if notify.On == models.SLAMissEvent {
			if _, ok := notify.Config["duration"]; !ok {
				continue
			}

			dur, err := time.ParseDuration(notify.Config["duration"])
			if err != nil {
				return 0, fmt.Errorf("failed to parse sla_miss duration %s: %w", notify.Config["duration"], err)
			}
			slaMissDurationInSec = int64(dur.Seconds())
		}
	}
	return slaMissDurationInSec, nil
}

func (m monitoringService) registerNewJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	slaMissDurationInSec, err := getSLADuration(jobSpec)
	if err != nil {
		return err
	}
	err = m.JobRunMetricsRepository.Save(ctx, event, namespaceSpec, jobSpec, slaMissDurationInSec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) updateJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	err := m.JobRunMetricsRepository.Update(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) getActiveJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	eventPayload := event.Value
	jobRunSpec, err := m.JobRunMetricsRepository.GetActiveJobRun(ctx, eventPayload["scheduled_at"].GetStringValue(), namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return jobRunSpec, err
	}
	return jobRunSpec, err
}

func (m monitoringService) registerTaskRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	_, err = m.TaskRunRepository.GetTaskRunIfExists(ctx, jobRunSpec)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.TaskRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.TaskRunRepository.Update(ctx, event, jobRunSpec)
}

func (m monitoringService) registerSensorRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	_, err = m.SensorRunRepository.GetSensorRunIfExists(ctx, jobRunSpec)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.SensorRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.SensorRunRepository.Update(ctx, event, jobRunSpec)
}
func (m monitoringService) registerHookRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	if _, err = m.HookRunRepository.GetHookRunIfExists(ctx, jobRunSpec); err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.HookRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.HookRunRepository.Update(ctx, event, jobRunSpec)
}

func (m monitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	m.logger.Info("event.Type", string(event.Type))
	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)
	m.logger.Info("eventPayloadString", string(eventPayloadString))

	switch event.Type {
	case models.JobStartEvent:
		return m.registerNewJobRun(ctx, event, namespaceSpec, jobSpec)
	case models.JobSuccessEvent, models.JobFailEvent:
		return m.updateJobRun(ctx, event, namespaceSpec, jobSpec)
	case models.TaskStartEvent, models.TaskSuccessEvent, models.TaskRetryEvent, models.TaskFailEvent:
		return m.registerTaskRunEvent(ctx, event, namespaceSpec, jobSpec)
	case models.SensorStartEvent, models.SensorSuccessEvent, models.SensorRetryEvent, models.SensorFailEvent:
		return m.registerSensorRunEvent(ctx, event, namespaceSpec, jobSpec)
	case models.HookStartEvent, models.HookSuccessEvent, models.HookRetryEvent, models.HookFailEvent:
		return m.registerHookRunEvent(ctx, event, namespaceSpec, jobSpec)
	}
	return nil
}

func NewMonitoringService(logger log.Logger,
	jobRunService JobRunService,
	jobRunMetricsRepository store.JobRunMetricsRepository,
	sensorRunRepository store.SensorRunRepository,
	hookRunRepository store.HookRunRepository,
	taskRunRepository store.TaskRunRepository) *monitoringService {
	return &monitoringService{
		logger:                  logger,
		JobRunService:           jobRunService,
		TaskRunRepository:       taskRunRepository,
		JobRunMetricsRepository: jobRunMetricsRepository,
		SensorRunRepository:     sensorRunRepository,
		HookRunRepository:       hookRunRepository,
	}
}
