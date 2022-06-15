package service

import (
	"context"
	"encoding/json"
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

const (
	airflowDateFormat = "2006-01-02T15:04:05Z"
)

func getSlaDuration(jobSpec models.JobSpec) (int64, error) {
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
	slaMissDurationInSec, err := getSlaDuration(jobSpec)
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

func (m monitoringService) getJobRunByAttempt(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	jobRunSpec, err := m.JobRunMetricsRepository.Get(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return jobRunSpec, err
	}
	return jobRunSpec, err
}

func (m monitoringService) registerJobRunFinish(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	// check if same job exixsts then , update its rerun count and last run time
	err := m.JobRunMetricsRepository.Update(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) registerTaskRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}

	if _, err := m.TaskRunRepository.GetTaskRunIfExists(ctx, event, jobRunSpec); err != nil {
		err = m.TaskRunRepository.Save(ctx, event, jobRunSpec)
	} else {
		err = m.TaskRunRepository.Update(ctx, event, jobRunSpec)
	}
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}

	return nil
}

func (m monitoringService) registerSensorRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}

	if _, err := m.SensorRunRepository.GetSensorRunIfExists(ctx, event, jobRunSpec); err != nil {
		m.logger.Info(err.Error())
		err = m.SensorRunRepository.Save(ctx, event, jobRunSpec)
	} else {
		err = m.SensorRunRepository.Update(ctx, event, jobRunSpec)
	}
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) registerHookRunEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	if _, err = m.HookRunRepository.GetHookRunIfExists(ctx, event, jobRunSpec); err != nil {
		err = m.HookRunRepository.Save(ctx, event, jobRunSpec)
	} else {
		err = m.HookRunRepository.Update(ctx, event, jobRunSpec)
	}
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	m.logger.Info("event.Type")
	m.logger.Info(string(event.Type))
	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)
	m.logger.Info("eventPayloadString")
	m.logger.Info(string(eventPayloadString))

	switch event.Type {
	case models.JobStartEvent:
		m.registerNewJobRun(ctx, event, namespaceSpec, jobSpec)
	case models.JobSuccessEvent, models.JobFailEvent:
		m.updateJobRun(ctx, event, namespaceSpec, jobSpec)
	case models.TaskStartEvent, models.TaskSuccessEvent, models.TaskRetryEvent, models.TaskFailEvent:
		m.registerTaskRunEvent(ctx, event, namespaceSpec, jobSpec)
	case models.SensorStartEvent, models.SensorSuccessEvent, models.SensorRetryEvent, models.SensorFailEvent:
		m.registerSensorRunEvent(ctx, event, namespaceSpec, jobSpec)
	case models.HookStartEvent, models.HookSuccessEvent, models.HookRetryEvent, models.HookFailEvent:
		m.registerHookRunEvent(ctx, event, namespaceSpec, jobSpec)
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
