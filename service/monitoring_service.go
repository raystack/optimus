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
	// TaskRunRepository   store.TaskRunRepository
	// SensorRunRepository store.SensorRunRepository
	// HookRunRepository   store.HookRunRepository
}

type MonitoringService interface {
	ProcessEvent(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec) error

	// GetJobRun(context.Context) error
	// GetSensorRun(context.Context) error
	// RegisterNewJobRun(context.Context, models.JobSpec, models.NamespaceSpec) error
	// RegisterNewSensorRun(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDestinationResponse, error)
	// RegisterNewTaskRun(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDestinationResponse, error)
	// RegisterNewHookRun(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDestinationResponse, error)
}

func (m monitoringService) registerNewJobRun(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	m.logger.Info("registerNewJobRun")
	err := m.JobRunMetricsRepository.Save(ctx, event, namespaceSpec, jobSpec)
	if err != nil {
		m.logger.Info(err.Error())
	}
	return nil
}

// func (m MonitoringDependencies) GetJobRun(ctx context.Context) error {

// 	return nil
// }

func (m monitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	switch event.Type {
	case models.JobStartEvent:
		m.registerNewJobRun(ctx, event, namespaceSpec, jobSpec)
		break
	}
	return nil
}

func NewMonitoringService(logger log.Logger, jobRunService JobRunService, jobRunMetricsRepository store.JobRunMetricsRepository) *monitoringService {
	return &monitoringService{
		logger:                  logger,
		JobRunService:           jobRunService,
		JobRunMetricsRepository: jobRunMetricsRepository,
	}
}
