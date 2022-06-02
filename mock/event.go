package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type MonitoringService struct {
	mock.Mock
}

func (srv *MonitoringService) ProcessEvent(ctx context.Context, event models.JobEvent) error {
	return srv.Called(ctx, event).Error(0)
}
