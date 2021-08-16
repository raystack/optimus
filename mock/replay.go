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

func (repo *ReplayRepository) GetByID(id uuid.UUID) (models.ReplaySpec, error) {
	args := repo.Called(id)
	return args.Get(0).(models.ReplaySpec), args.Error(1)
}

func (repo *ReplayRepository) Insert(replay *models.ReplaySpec) error {
	return repo.Called(&models.ReplaySpec{
		ID:        replay.ID,
		Job:       replay.Job,
		StartDate: replay.StartDate,
		EndDate:   replay.EndDate,
		Status:    replay.Status,
		Message:   replay.Message,
		CreatedAt: replay.CreatedAt,
	}).Error(0)
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

func (repo *ReplayRepository) GetByProjectIDAndStatus(projectID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	args := repo.Called(projectID, status)
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

func (rm *ReplayManager) Replay(ctx context.Context, reqInput models.ReplayRequest) (string, error) {
	args := rm.Called(ctx, reqInput)
	return args.Get(0).(string), args.Error(1)
}

func (rm *ReplayManager) Init() {
	rm.Called()
	return
}

func (rm *ReplayManager) GetReplay(uuid uuid.UUID) (models.ReplaySpec, error) {
	args := rm.Called(uuid)
	return args.Get(0).(models.ReplaySpec), args.Error(1)
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
		finish: make(chan bool, 0),
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
