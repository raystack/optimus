package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
)

type monitoringService struct {
	JobRunMetricsRepository store.JobRunMetricsRepository
	TaskRunRepository       store.TaskRunRepository
	SensorRunRepository     store.SensorRunRepository
	HookRunRepository       store.HookRunRepository
}

type MonitoringService interface {
	ProcessEvent(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec) error
	GetJobRunByScheduledAt(context.Context, models.NamespaceSpec, models.JobSpec, time.Time) (models.JobRunSpec, error)
	GetJobRunByRunID(context.Context, uuid.UUID, models.JobSpec) (models.JobRunSpec, error)
}

func (m monitoringService) GetJobRunByScheduledAt(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, scheduledAt time.Time) (models.JobRunSpec, error) {
	return m.JobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, scheduledAt.Format(store.ISODateFormat), namespaceSpec, jobSpec)
}
func (m monitoringService) GetJobRunByRunID(ctx context.Context, jobRunID uuid.UUID, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	return m.JobRunMetricsRepository.GetByID(ctx, jobRunID, jobSpec)
}

func (m monitoringService) registerNewJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	slaDefinitionInSec, err := jobSpec.SLADuration()
	if err != nil {
		return err
	}
	return m.JobRunMetricsRepository.Save(ctx, event, namespaceSpec, jobSpec, slaDefinitionInSec)
}

func (m monitoringService) updateJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return m.JobRunMetricsRepository.Update(ctx, event, namespaceSpec, jobSpec)
}

func (m monitoringService) GetLatestJobRunByScheduledTime(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	eventPayload := event.Value
	jobRunSpec, err := m.JobRunMetricsRepository.GetLatestJobRunByScheduledTime(ctx, eventPayload["scheduled_at"].GetStringValue(), namespaceSpec, jobSpec)
	if err != nil {
		return jobRunSpec, err
	}
	return jobRunSpec, err
}

func (m monitoringService) registerTaskRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.GetLatestJobRunByScheduledTime(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	_, err = m.TaskRunRepository.GetTaskRun(ctx, jobRunSpec)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.TaskRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.TaskRunRepository.Update(ctx, event, jobRunSpec)
}

func (m monitoringService) registerSensorRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.GetLatestJobRunByScheduledTime(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	_, err = m.SensorRunRepository.GetSensorRun(ctx, jobRunSpec)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.SensorRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.SensorRunRepository.Update(ctx, event, jobRunSpec)
}
func (m monitoringService) registerHookRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.GetLatestJobRunByScheduledTime(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		return err
	}
	if _, err = m.HookRunRepository.GetHookRun(ctx, jobRunSpec); err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return m.HookRunRepository.Save(ctx, event, jobRunSpec)
		}
		return err
	}
	return m.HookRunRepository.Update(ctx, event, jobRunSpec)
}

func (m monitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
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

func NewMonitoringService(jobRunMetricsRepository store.JobRunMetricsRepository,
	sensorRunRepository store.SensorRunRepository,
	hookRunRepository store.HookRunRepository,
	taskRunRepository store.TaskRunRepository) *monitoringService {
	return &monitoringService{
		TaskRunRepository:       taskRunRepository,
		JobRunMetricsRepository: jobRunMetricsRepository,
		SensorRunRepository:     sensorRunRepository,
		HookRunRepository:       hookRunRepository,
	}
}
