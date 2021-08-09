package job

import (
	"context"
	"sync"
	"time"

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
	airflowSyncInterval = "@every 5m"
)

type ReplayManagerConfig struct {
	NumWorkers    int
	WorkerTimeout time.Duration
	RunTimeout    time.Duration
}

// Manager for replaying operation(s).
// Offers an asynchronous interface to pipeline, with a fixed size request queue
// Each replay request is handled by a replay worker and the number of parallel replay workers
// can be provided through configuration.
type Manager struct {
	// wait group to synchronise on workers
	wg sync.WaitGroup
	mu sync.Mutex

	uuidProvider utils.UUIDProvider
	config       ReplayManagerConfig

	// request queue, used by workers
	requestQ chan models.ReplayRequest
	// request map, used for verifying if a request is
	// in queue without actually consuming it
	requestMap map[uuid.UUID]bool

	//request worker
	replayWorker ReplayWorker

	replaySpecRepoFac ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
	replayValidator   ReplayValidator
	replaySyncer      ReplaySyncer
	syncerScheduler   *cron.Cron
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
	// try sending the job request down the request queue
	// if full return error indicating that we don't have capacity
	// to process this request at the moment
	select {
	case m.requestQ <- reqInput:
		m.mu.Lock()
		//request pushed to worker
		m.requestMap[reqInput.ID] = true
		m.mu.Unlock()
		if err = replaySpecRepo.Insert(&replay); err != nil {
			return "", err
		}
		return reqInput.ID.String(), nil
	default:
		return "", ErrRequestQueueFull
	}
}

// start a worker goroutine that runs the deployment pipeline in background
func (m *Manager) spawnServiceWorker() {
	defer m.wg.Done()

	for reqInput := range m.requestQ {
		logger.I("worker picked up the request for ", reqInput)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := m.replayWorker.Process(ctx, reqInput); err != nil {
			logger.E(errors.Wrap(err, "worker failed to process"))
			cancelCtx()
		}
		cancelCtx()
	}
}

// AirflowSyncer to sync for latest replay status
func (m *Manager) AirflowSyncer() {
	logger.D("start synchronizing replays...")
	ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
	defer cancelCtx()
	if err := m.replaySyncer.Sync(ctx, m.config.RunTimeout); err != nil {
		logger.E(errors.Wrap(err, "syncer failed to process"))
	}
	logger.I("replays synced")
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
	m.syncerScheduler.AddFunc(airflowSyncInterval, m.AirflowSyncer)
	m.syncerScheduler.Start()

	logger.I("starting replay workers")
	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		go m.spawnServiceWorker()
	}
}

// NewManager constructs a new instance of Manager
func NewManager(worker ReplayWorker, replaySpecRepoFac ReplaySpecRepoFactory, uuidProvider utils.UUIDProvider,
	config ReplayManagerConfig, scheduler models.SchedulerUnit, validator ReplayValidator, syncer ReplaySyncer) *Manager {
	mgr := &Manager{
		replayWorker:      worker,
		requestMap:        make(map[uuid.UUID]bool),
		config:            config,
		requestQ:          make(chan models.ReplayRequest, 0),
		replaySpecRepoFac: replaySpecRepoFac,
		uuidProvider:      uuidProvider,
		scheduler:         scheduler,
		replayValidator:   validator,
		replaySyncer:      syncer,
		syncerScheduler: cron.New(cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		)),
	}
	mgr.Init()
	return mgr
}

// GetReplay using UUID
func (m *Manager) GetReplay(replayUUID uuid.UUID) (models.ReplaySpec, error) {
	replaySpecRepo := m.replaySpecRepoFac.New()
	replaySpec, err := replaySpecRepo.GetByID(replayUUID)
	if err != nil {
		return models.ReplaySpec{}, err
	}

	return replaySpec, nil
}

// GetRunsStatus
func (m *Manager) GetRunStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate time.Time,
	endDate time.Time, jobName string) ([]models.JobStatus, error) {
	batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
	return m.scheduler.GetDagRunStatus(ctx, projectSpec, jobName, startDate, batchEndDate, schedulerBatchSize)
}
