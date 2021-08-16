package job

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/store"

	"github.com/robfig/cron/v3"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"github.com/pkg/errors"
)

var (
	// ErrRequestQueueFull signifies that the deployment manager's
	// request queue is full
	ErrRequestQueueFull = errors.New("request queue is full")
	// ErrConflictedJobRun signifies other replay job / dependency run is active or instance already running
	ErrConflictedJobRun = errors.New("conflicted job run found")
	//ReplayRunTimeout signifies type of replay failure caused by timeout
	ReplayRunTimeout = "long running replay timeout"
	// TimestampLogFormat format of a timestamp will be used in logs
	TimestampLogFormat = "2006-01-02T15:04:05+00:00"
	// schedulerBatchSize number of run instances to be checked per request
	schedulerBatchSize = 100
)

const (
	syncInterval = "@every 5m"
)

type ReplayManagerConfig struct {
	NumWorkers    int
	WorkerTimeout time.Duration
	RunTimeout    time.Duration
}

type ReplayWorkerFactory interface {
	New() ReplayWorker
}

type ReplaySyncer interface {
	Sync(context.Context, time.Duration) error
}

type ReplayValidator interface {
	Validate(context.Context, store.ReplaySpecRepository, models.ReplayRequest, *tree.TreeNode) error
}

// Manager for replaying operation(s).
// Offers an asynchronous interface to pipeline, with a fixed size request queue
// Each replay request is handled by a replay worker and the number of parallel replay workers
// can be provided through configuration.
type Manager struct {
	// wait group to synchronise on workers
	wg sync.WaitGroup

	uuidProvider utils.UUIDProvider
	config       ReplayManagerConfig

	// request queue, used by workers
	requestQ chan models.ReplayRequest

	replayWorkerFactory ReplayWorkerFactory
	replaySpecRepoFac   ReplaySpecRepoFactory
	scheduler           models.SchedulerUnit
	replayValidator     ReplayValidator
	replaySyncer        ReplaySyncer
	syncerScheduler     *cron.Cron

	numBusyWorkers int32
}

// Replay a request asynchronously, returns a replay id that can
// can be used to query its status
func (m *Manager) Replay(ctx context.Context, reqInput models.ReplayRequest) (string, error) {
	replaySpecRepo := m.replaySpecRepoFac.New()

	replayTree, err := prepareReplayExecutionTree(reqInput)
	if err != nil {
		return "", err
	}
	if err := m.replayValidator.Validate(ctx, replaySpecRepo, reqInput, replayTree); err != nil {
		return "", err
	}

	uuidOb, err := m.uuidProvider.NewUUID()
	if err != nil {
		return "", err
	}
	reqInput.ID = uuidOb

	// save replay request and mark status as accepted
	replay := models.ReplaySpec{
		ID:            uuidOb,
		Job:           reqInput.Job,
		StartDate:     reqInput.Start,
		EndDate:       reqInput.End,
		Status:        models.ReplayStatusAccepted,
		ExecutionTree: replayTree,
	}

	// no need to save this request if all workers are busy
	if int(atomic.LoadInt32(&m.numBusyWorkers)) == m.config.NumWorkers {
		return "", ErrRequestQueueFull
	}

	if err = replaySpecRepo.Insert(&replay); err != nil {
		return "", err
	}

	m.requestQ <- reqInput
	return reqInput.ID.String(), nil
}

// start a worker goroutine that runs the replay in background
func (m *Manager) spawnServiceWorker(worker ReplayWorker) {
	defer m.wg.Done()

	for reqInput := range m.requestQ {
		atomic.AddInt32(&m.numBusyWorkers, 1)

		logger.I("worker picked up the request for ", reqInput.ID)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := worker.Process(ctx, reqInput); err != nil {
			logger.E(errors.Wrap(err, "worker failed to process"))
		}
		cancelCtx()

		atomic.AddInt32(&m.numBusyWorkers, -1)
	}
}

// SchedulerSyncer to sync for latest replay status
func (m *Manager) SchedulerSyncer() {
	if m.replaySyncer == nil {
		return
	}

	logger.D("start synchronizing replays...")
	ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
	defer cancelCtx()
	if err := m.replaySyncer.Sync(ctx, m.config.RunTimeout); err != nil {
		logger.E(errors.Wrap(err, "syncer failed to process"))
	}
	logger.D("replays synced")
}

// GetReplay using UUID
func (m *Manager) GetReplay(replayUUID uuid.UUID) (models.ReplaySpec, error) {
	return m.replaySpecRepoFac.New().GetByID(replayUUID)
}

// GetRunsStatus
func (m *Manager) GetRunStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate time.Time,
	endDate time.Time, jobName string) ([]models.JobStatus, error) {
	batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
	return m.scheduler.GetDagRunStatus(ctx, projectSpec, jobName, startDate, batchEndDate, schedulerBatchSize)
}

//Close stops consuming any new request
func (m *Manager) Close() error {
	if m.requestQ != nil {
		//stop accepting any more requests
		close(m.requestQ)
	}

	//wait for request worker to finish
	m.wg.Wait()

	if m.syncerScheduler != nil {
		//wait for syncer to finish
		<-m.syncerScheduler.Stop().Done()
	}
	return nil
}

func (m *Manager) Init() {
	_, err := m.syncerScheduler.AddFunc(syncInterval, m.SchedulerSyncer)
	if err != nil {
		logger.F(err)
	}
	m.syncerScheduler.Start()

	logger.I("starting replay workers")
	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		worker := m.replayWorkerFactory.New()
		go m.spawnServiceWorker(worker)
	}
}

// NewManager constructs a new instance of Manager
func NewManager(workerFact ReplayWorkerFactory, replaySpecRepoFac ReplaySpecRepoFactory, uuidProvider utils.UUIDProvider,
	config ReplayManagerConfig, scheduler models.SchedulerUnit, validator ReplayValidator, syncer ReplaySyncer) *Manager {
	mgr := &Manager{
		replayWorkerFactory: workerFact,
		config:              config,
		requestQ:            make(chan models.ReplayRequest, 0),
		replaySpecRepoFac:   replaySpecRepoFac,
		uuidProvider:        uuidProvider,
		scheduler:           scheduler,
		replayValidator:     validator,
		replaySyncer:        syncer,
		numBusyWorkers:      0,
		syncerScheduler: cron.New(cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		)),
	}
	mgr.Init()
	return mgr
}
