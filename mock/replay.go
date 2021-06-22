package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type ReplayRepository struct {
	mock.Mock
}

func (repo *ReplayRepository) GetByID(id uuid.UUID) (models.ReplaySpec, error) {
	args := repo.Called(id)
	return args.Get(0).(models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) Insert(replay *models.ReplaySpec) error {
	return repo.Called(replay).Error(0)
}

func (repo *ReplayRepository) UpdateStatus(replayID uuid.UUID, status string, message models.ReplayMessage) error {
	return repo.Called(replayID, status, message).Error(0)
}

type ReplaySpecRepoFactory struct {
	mock.Mock
}

func (fac *ReplaySpecRepoFactory) New(jobSpec models.JobSpec) store.ReplaySpecRepository {
	return fac.Called(jobSpec).Get(0).(store.ReplaySpecRepository)
}

type ReplayManager struct {
	mock.Mock
}

func (rm *ReplayManager) Replay(reqInput *models.ReplayRequestInput) (string, error) {
	args := rm.Called(reqInput)
	return args.Get(0).(string), args.Error(1)
}

func (rm *ReplayManager) Init() {
	rm.Called()
	return
}

type ReplayWorker struct {
	mock.Mock
}

func (rm *ReplayWorker) Process(ctx context.Context, replayRequest *models.ReplayRequestInput) error {
	args := rm.Called(ctx, replayRequest)
	return args.Error(0)
}
