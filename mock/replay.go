package mock

import (
	"context"
	"time"

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

func (repo *ReplayRepository) GetByStatus(status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(status)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) GetByJobIDAndStatus(jobID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(jobID, status)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
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

func (rm *ReplayManager) Replay(ctx context.Context, reqInput *models.ReplayRequest) (string, error) {
	args := rm.Called(ctx, reqInput)
	return args.Get(0).(string), args.Error(1)
}

func (rm *ReplayManager) Init() {
	rm.Called()
	return
}

func (rm *ReplayManager) GetReplay(uuid uuid.UUID) (*models.ReplaySpec, error) {
	args := rm.Called(uuid)
	return args.Get(0).(*models.ReplaySpec), args.Error(1)
}

func (rm *ReplayManager) GetRunStatus(ctx context.Context, reqInput *models.ReplayRequest, jobName string) ([]models.JobStatus, error) {
	args := rm.Called(ctx, reqInput, jobName)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

type ReplayWorker struct {
	mock.Mock
}

func (rm *ReplayWorker) Process(ctx context.Context, replayRequest *models.ReplayRequest) error {
	args := rm.Called(ctx, replayRequest)
	return args.Error(0)
}

type ReplayValidator struct {
	mock.Mock
}

func (rv *ReplayValidator) Validate(ctx context.Context, replaySpecRepo store.ReplaySpecRepository, replayRequest *models.ReplayRequest) error {
	args := rv.Called(ctx, replaySpecRepo, replayRequest)
	return args.Error(0)
}

type ReplaySyncer struct {
	mock.Mock
}

func (rs *ReplaySyncer) Sync(context context.Context, runTimeout time.Duration) error {
	args := rs.Called(context, runTimeout)
	return args.Error(0)
}
