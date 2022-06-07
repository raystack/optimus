package service

import (
	"context"

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

func (m monitoringService) registerNewJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	// check if same job exixsts then , update its rerun count and last run time
	err := m.JobRunMetricsRepository.Save(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) updateJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	// check if same job exixsts then , update its rerun count and last run time
	err := m.JobRunMetricsRepository.Update(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) getActiveJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	// check if same job exixsts then , update its rerun count and last run time
	eventPayload := event.Value

	jobRunSpec, err := m.JobRunMetricsRepository.GetActiveJobRun(ctx, eventPayload["scheduled_at"].GetStringValue(), namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return jobRunSpec, err
	}
	return jobRunSpec, err
}

func (m monitoringService) getJobRunByAttempt(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	// check if same job exixsts then , update its rerun count and last run time
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

func (m monitoringService) registerTaskRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	// check if same job exixsts then , update its rerun count and last run time

	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	// scheduledAtTimeStamp, err := time.Parse(airflowDateFormat, eventPayload["scheduled_at"].GetStringValue())

	if err != nil {
		m.logger.Info(err.Error())
		return err
	}

	_, err = m.TaskRunRepository.GetTaskRunIfExists(ctx, event, namespaceSpec, jobSpec, jobRunSpec)
	if err != nil {
		// task registered already
		err = m.TaskRunRepository.Update(ctx, event, namespaceSpec, jobSpec, jobRunSpec)
		// handle error
	} else {
		err = m.TaskRunRepository.Save(ctx, event, namespaceSpec, jobSpec, jobRunSpec)
	}
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}

	return nil
}

func (m monitoringService) UpdateTaskRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	// check if same job exixsts then , update its rerun count and last run time
	jobRunSpec, err := m.getActiveJobRun(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	err = m.TaskRunRepository.Update(ctx, event, namespaceSpec, jobSpec, jobRunSpec)
	if err != nil {
		m.logger.Info(err.Error())
		return err
	}
	return nil
}

func (m monitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	switch event.Type {
	case models.JobStartEvent:
		m.registerNewJobRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.JobSuccessEvent:
		m.updateJobRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.JobFailEvent:
		m.updateJobRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.TaskStartEvent:
		m.registerTaskRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.TaskSuccessEvent:
		m.UpdateTaskRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.TaskRetryEvent:
		m.UpdateTaskRun(ctx, event, namespaceSpec, jobSpec)
		break
	case models.TaskFailEvent:
		m.UpdateTaskRun(ctx, event, namespaceSpec, jobSpec)
		break
	}
	return nil
}

func NewMonitoringService(logger log.Logger, jobRunService JobRunService, jobRunMetricsRepository store.JobRunMetricsRepository, taskRunRepository store.TaskRunRepository) *monitoringService {
	return &monitoringService{
		logger:                  logger,
		JobRunService:           jobRunService,
		TaskRunRepository:       taskRunRepository,
		JobRunMetricsRepository: jobRunMetricsRepository,
	}
}
