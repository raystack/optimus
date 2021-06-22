package mock

import (
	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type ReplayRepository struct {
	mock.Mock
}

func (repo *ReplayRepository) GetByID(id uuid.UUID) (models.ReplaySpec, error) {
	called := repo.Called(id)
	return called.Get(0).(models.ReplaySpec), called.Error(1)
}

func (repo *ReplayRepository) Insert(replay *models.ReplaySpec) error {
	return repo.Called(replay).Error(0)
}

func (repo *ReplayRepository) UpdateStatus(replayID uuid.UUID, status, message string) error {
	return repo.Called(replayID, status, message).Error(0)
}
