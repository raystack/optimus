package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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
	// ReplayStatusToValidate signifies list of status to be used when checking active replays
	ReplayStatusToValidate = []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}
)

type ReplayManagerConfig struct {
	NumWorkers    int
	WorkerTimeout time.Duration
	RunTimeout    time.Duration
}

type ReplayManager interface {
	Init()
	Replay(context.Context, *models.ReplayWorkerRequest) (string, error)
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
	requestQ chan *models.ReplayWorkerRequest
	// request map, used for verifying if a request is
	// in queue without actually consuming it
	requestMap map[uuid.UUID]bool

	//request worker
	replayWorker ReplayWorker

	replaySpecRepoFac ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
}

// Replay a request asynchronously, returns a replay id that can
// can be used to query its status
func (m *Manager) Replay(ctx context.Context, reqInput *models.ReplayWorkerRequest) (string, error) {
	replaySpecRepo := m.replaySpecRepoFac.New(reqInput.Job)

	err := m.validate(ctx, replaySpecRepo, reqInput)
	if err != nil {
		return "", err
	}

	uuidOb, err := m.uuidProvider.NewUUID()
	if err != nil {
		return "", err
	}
	reqInput.ID = uuidOb

	// save replay request and mark status as accepted
	replay := models.ReplaySpec{
		ID:        uuidOb,
		Job:       reqInput.Job,
		StartDate: reqInput.Start,
		EndDate:   reqInput.End,
		Status:    models.ReplayStatusAccepted,
	}
	if err = replaySpecRepo.Insert(&replay); err != nil {
		return "", err
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

		return reqInput.ID.String(), nil
	default:
		return "", ErrRequestQueueFull
	}
}

func (m *Manager) validate(ctx context.Context, replaySpecRepo store.ReplaySpecRepository, reqInput *models.ReplayWorkerRequest) error {
	reqReplayTree, err := prepareTree(reqInput)
	if err != nil {
		return err
	}
	reqReplayNodes := reqReplayTree.GetAllNodes()

	if !reqInput.Force {
		//check if this dag have running instance in the scheduler
		err = m.validateRunningInstance(ctx, reqReplayNodes, reqInput)
		if err != nil {
			return err
		}

		//check another replay active for this dag
		activeReplaySpecs, err := replaySpecRepo.GetByStatus(ReplayStatusToValidate)
		if err != nil {
			if err == store.ErrResourceNotFound {
				return nil
			}
			return err
		}
		return validateReplayJobsConflict(activeReplaySpecs, reqInput, reqReplayNodes)
	}
	//check and cancel if found conflicted replays for same job ID
	return cancelConflictedReplays(replaySpecRepo, reqInput)
}

func cancelConflictedReplays(replaySpecRepo store.ReplaySpecRepository, reqInput *models.ReplayWorkerRequest) error {
	duplicatedReplaySpecs, err := replaySpecRepo.GetByJobIDAndStatus(reqInput.Job.ID, ReplayStatusToValidate)
	if err != nil {
		if err == store.ErrResourceNotFound {
			return nil
		}
		return err
	}
	for _, replaySpec := range duplicatedReplaySpecs {
		if err := replaySpecRepo.UpdateStatus(replaySpec.ID, models.ReplayStatusCancelled, models.ReplayMessage{
			Type:    ErrConflictedJobRun.Error(),
			Message: fmt.Sprintf("force started replay with ID: %s", reqInput.ID),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) validateRunningInstance(ctx context.Context, reqReplayNodes []*tree.TreeNode, reqInput *models.ReplayWorkerRequest) error {
	requestBatchSize := 100
	for _, reqReplayNode := range reqReplayNodes {
		batchEndDate := reqInput.End.AddDate(0, 0, 1)
		jobStatusAllRuns, err := m.scheduler.GetDagRunStatus(ctx, reqInput.Project, reqInput.Job.Name, reqInput.Start, batchEndDate, requestBatchSize)
		if err != nil {
			return err
		}
		for _, jobStatus := range jobStatusAllRuns {
			if reqReplayNode.Runs.Contains(jobStatus.ScheduledAt) && jobStatus.State == models.JobStatusStateRunning {
				return ErrConflictedJobRun
			}
		}
	}
	return nil
}

func validateReplayJobsConflict(activeReplaySpecs []models.ReplaySpec, reqInput *models.ReplayWorkerRequest,
	reqReplayNodes []*tree.TreeNode) error {
	for _, activeSpec := range activeReplaySpecs {
		activeReplayWorkerRequest := &models.ReplayWorkerRequest{
			ID:         activeSpec.ID,
			Job:        activeSpec.Job,
			Start:      activeSpec.StartDate,
			End:        activeSpec.EndDate,
			Project:    reqInput.Project,
			JobSpecMap: reqInput.JobSpecMap,
		}
		activeTree, err := prepareTree(activeReplayWorkerRequest)
		if err != nil {
			return err
		}
		activeNodes := activeTree.GetAllNodes()
		return checkAnyConflictedDags(activeNodes, reqReplayNodes)
	}
	return nil
}

func checkAnyConflictedDags(activeNodes []*tree.TreeNode, reqReplayNodes []*tree.TreeNode) error {
	activeNodesMap := make(map[string]*tree.TreeNode)
	for _, activeNodes := range activeNodes {
		activeNodesMap[activeNodes.GetName()] = activeNodes
	}

	for _, reqNode := range reqReplayNodes {
		if _, ok := activeNodesMap[reqNode.GetName()]; ok {
			if err := checkAnyConflictedRuns(activeNodesMap[reqNode.GetName()], reqNode); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkAnyConflictedRuns(activeNode *tree.TreeNode, reqNode *tree.TreeNode) error {
	for _, reqNodeRun := range reqNode.Runs.Values() {
		if activeNode.Runs.Contains(reqNodeRun.(time.Time)) {
			return ErrConflictedJobRun
		}
	}
	return nil
}

// start a worker goroutine that runs the deployment pipeline in background
func (m *Manager) spawnServiceWorker() {
	defer m.wg.Done()

	for reqInput := range m.requestQ {
		logger.I("worker picked up the request for ", reqInput.Job.Name)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := m.replayWorker.Process(ctx, reqInput); err != nil {
			//do something about this error
			logger.E(errors.Wrap(err, "worker failed to process"))
			cancelCtx()
		}
		cancelCtx()
	}
}

//Close stops consuming any new request
func (m *Manager) Close() error {
	if m.requestQ != nil {
		//stop accepting any more requests
		close(m.requestQ)
	}

	//wait for request worker to finish
	m.wg.Wait()

	return nil
}

func (m *Manager) Init() {
	m.shuttingDownLongRunningReplay()

	logger.I("starting replay workers")
	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		go m.spawnServiceWorker()
	}
}

func (m *Manager) shuttingDownLongRunningReplay() {
	replaySpecRepo := m.replaySpecRepoFac.New(models.JobSpec{})
	runningReplaySpecs, err := replaySpecRepo.GetByStatus(ReplayStatusToValidate)
	if err != nil {
		logger.I(fmt.Sprintf("shutting down long running replay jobs failed: %s", err))
	}
	for _, runningReplaySpec := range runningReplaySpecs {
		runningTime := time.Now().Sub(runningReplaySpec.CreatedAt)
		if runningTime > m.config.RunTimeout {
			if updateStatusErr := replaySpecRepo.UpdateStatus(runningReplaySpec.ID, models.ReplayStatusFailed, models.ReplayMessage{
				Type:    ReplayRunTimeout,
				Message: fmt.Sprintf("replay has been running since %s", runningReplaySpec.CreatedAt.UTC().Format(TimestampLogFormat)),
			}); updateStatusErr != nil {
				logger.I(fmt.Sprintf("shutting down long running replay jobs failed: %s", updateStatusErr))
			}
		}
	}
}

// NewManager constructs a new instance of Manager
func NewManager(worker ReplayWorker, replaySpecRepoFac ReplaySpecRepoFactory, uuidProvider utils.UUIDProvider,
	config ReplayManagerConfig, scheduler models.SchedulerUnit) *Manager {
	mgr := &Manager{
		replayWorker:      worker,
		requestMap:        make(map[uuid.UUID]bool),
		config:            config,
		requestQ:          make(chan *models.ReplayWorkerRequest, 0),
		replaySpecRepoFac: replaySpecRepoFac,
		uuidProvider:      uuidProvider,
		scheduler:         scheduler,
	}
	mgr.Init()
	return mgr
}
