package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type MonitoringService struct {
	mock.Mock
}

func (srv *MonitoringService) ProcessEvent(ctx context.Context, event models.JobEvent,  namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return srv.Called(ctx, event,namespaceSpec,jobSpec).Error(0)
}
