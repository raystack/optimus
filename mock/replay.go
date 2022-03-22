package mock

import (
	"context"
	"time"

	"github.com/odpf/optimus/job"

	"github.com/odpf/optimus/core/tree"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type ReplayRepository struct {
	mock.Mock
}

func (repo *ReplayRepository) GetByID(ctx context.Context, id uuid.UUID) (models.ReplaySpec, error) {
	args := repo.Called(ctx, id)
	return args.Get(0).(models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) Insert(ctx context.Context, replay *models.ReplaySpec) error {
	return repo.Called(ctx, &models.ReplaySpec{
		ID:        replay.ID,
		Job:       replay.Job,
		StartDate: replay.StartDate,
		EndDate:   replay.EndDate,
		Status:    replay.Status,
		Message:   replay.Message,
		CreatedAt: replay.CreatedAt,
	}).Error(0)
}

func (repo *ReplayRepository) UpdateStatus(ctx context.Context, replayID uuid.UUID, status string, message models.ReplayMessage) error {
	return repo.Called(ctx, replayID, status, message).Error(0)
}

func (repo *ReplayRepository) GetByStatus(ctx context.Context, status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(ctx, status)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) GetByJobIDAndStatus(ctx context.Context, jobID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(ctx, jobID, status)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) GetByProjectIDAndStatus(ctx context.Context, projectID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(ctx, projectID, status)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) GetByProjectID(ctx context.Context, projectID uuid.UUID) ([]models.ReplaySpec, error) {
	args := repo.Called(ctx, projectID)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

type ReplaySpecRepoFactory struct {
	mock.Mock
}

func (fac *ReplaySpecRepoFactory) New() store.ReplaySpecRepository {
	return fac.Called().Get(0).(store.ReplaySpecRepository)
}

type ReplayManager struct {
	mock.Mock
}

func (rm *ReplayManager) Replay(ctx context.Context, reqInput models.ReplayRequest) (models.ReplayResult, error) {
	args := rm.Called(ctx, reqInput)
	return args.Get(0).(models.ReplayResult), args.Error(1)
}

func (rm *ReplayManager) Init() {
	rm.Called()
}

func (rm *ReplayManager) GetReplay(ctx context.Context, uuid uuid.UUID) (models.ReplaySpec, error) {
	args := rm.Called(ctx, uuid)
	return args.Get(0).(models.ReplaySpec), args.Error(1)
}

func (rm *ReplayManager) GetReplayList(ctx context.Context, projectUUID uuid.UUID) ([]models.ReplaySpec, error) {
	args := rm.Called(ctx, projectUUID)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (rm *ReplayManager) GetRunStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate time.Time,
	endDate time.Time, jobName string) ([]models.JobStatus, error) {
	args := rm.Called(ctx, projectSpec, startDate, endDate, jobName)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

type ReplayWorkerFactoryIndexed struct {
	mock.Mock
	Workers []interface{}
	times   int
}

func (rm *ReplayWorkerFactoryIndexed) New() job.ReplayWorker {
	w := rm.Workers[rm.times]
	rm.times++
	rm.Called()
	return w.(job.ReplayWorker)
}

type ReplayWorkerFactory struct {
	mock.Mock
}

func (rm *ReplayWorkerFactory) New() job.ReplayWorker {
	args := rm.Called()
	return args.Get(0).(job.ReplayWorker)
}

type ReplayWorker struct {
	mock.Mock
	finish chan bool
}

func NewReplayWorker() *ReplayWorker {
	return &ReplayWorker{
		finish: make(chan bool),
	}
}

func (rm *ReplayWorker) Process(ctx context.Context, replayRequest models.ReplayRequest) error {
	//mock processing time for concurrent replay call testing
	args := rm.Called(ctx, replayRequest)
	<-rm.finish
	return args.Error(0)
}

func (rm *ReplayWorker) Close() error {
	close(rm.finish)
	return nil
}

type ReplayValidator struct {
	mock.Mock
}

func (rv *ReplayValidator) Validate(ctx context.Context, replaySpecRepo store.ReplaySpecRepository,
	replayRequest models.ReplayRequest, replayTree *tree.TreeNode) error {
	args := rv.Called(ctx, replaySpecRepo, replayRequest, replayTree)
	return args.Error(0)
}

type ReplaySyncer struct {
	mock.Mock
}

func (rs *ReplaySyncer) Sync(context context.Context, runTimeout time.Duration) error {
	args := rs.Called(context, runTimeout)
	return args.Error(0)
}
