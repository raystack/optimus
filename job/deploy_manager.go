package job

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"sync"
	"sync/atomic"
	"time"
)

type DeployManager interface {
	Init()
	Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error)
	GetStatus(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error)
}

type deployManager struct {
	// wait group to synchronise on workers
	wg sync.WaitGroup

	// request queue, used by workers
	requestQ chan models.DeployRequest

	l log.Logger

	config DeployManagerConfig

	deployerCapacity int32
	deployer         Deployer

	uuidProvider utils.UUIDProvider

	deployRepository store.JobDeploymentRepository
}

type DeployManagerConfig struct {
	NumWorkers    int
	WorkerTimeout time.Duration
	//RunTimeout    time.Duration
}

type DeployerFactory interface {
	New() Deployer
}

func (m *deployManager) Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error) {
	// Check deployment status for the requested Project
	jobDeployment, err := m.deployRepository.GetByStatusAndProjectID(ctx, models.JobDeploymentStatusInQueue, projectSpec.ID)
	if err != nil {
		if !errors.Is(err, store.ErrResourceNotFound) {
			return models.DeploymentID{}, err
		}
	}

	// Will return with Deploy ID if it is already there in the queue
	if jobDeployment.ID != models.DeploymentID(uuid.Nil) {
		return jobDeployment.ID, nil
	}

	// Insert into job_deployment table
	newDeploymentID, err := m.uuidProvider.NewUUID()
	if err != nil {
		return models.DeploymentID{}, err
	}

	newDeployment := models.JobDeployment{
		ID:      models.DeploymentID(newDeploymentID),
		Project: projectSpec,
		Status:  models.JobDeploymentStatusCreated,
		Details: models.JobDeploymentDetail{},
	}
	if err = m.deployRepository.Save(ctx, newDeployment); err != nil {
		return models.DeploymentID{}, err
	}

	// Push to deployer using Deploy ID, let it run asynchronously
	deployRequest := models.DeployRequest{
		ID:      newDeploymentID,
		Project: projectSpec,
	}

	select {
	case m.requestQ <- deployRequest:
		fmt.Println("pushed to deployer")
		newDeployment.Status = models.JobDeploymentStatusInQueue
		if err = m.deployRepository.UpdateByID(ctx, newDeployment); err != nil {
			return models.DeploymentID{}, err
		}
	default:
		// failed to push to queue
		// can be because of limit of workers
		fmt.Println("failed to push to queue")
	}

	// Return with the new Deploy ID
	return newDeployment.ID, nil
}

func (m *deployManager) GetStatus(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	return m.deployRepository.GetByID(ctx, deployID)
}

func (m *deployManager) Init() {
	m.l.Info("starting deployers", "count", m.config.NumWorkers)
	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		go m.spawnDeployer(m.deployer)
	}

	// wait until all workers are ready
	for {
		if int(atomic.LoadInt32(&m.deployerCapacity)) == m.config.NumWorkers {
			break
		}
		time.Sleep(time.Millisecond * 50) //nolint: gomnd
	}
}

// start a deployer goroutine that runs the deployment in background
func (m *deployManager) spawnDeployer(deployer Deployer) {
	defer m.wg.Done()
	atomic.AddInt32(&m.deployerCapacity, 1)
	for reqInput := range m.requestQ {
		atomic.AddInt32(&m.deployerCapacity, -1)

		m.l.Info("deployer picked up the request", "request id", reqInput.ID)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := deployer.Deploy(ctx, reqInput); err != nil {
			m.l.Error("deployment worker failed to process", "error", err)
		}
		cancelCtx()

		atomic.AddInt32(&m.deployerCapacity, 1)
	}
}

// Close stops consuming any new request
func (m *deployManager) Close() error { // nolint: unparam
	if m.requestQ != nil {
		// stop accepting any more requests
		close(m.requestQ)
	}
	// wait for request worker to finish
	m.wg.Wait()

	return nil
}

// NewDeployManager constructs a new instance of Job Deployment Manager
func NewDeployManager(l log.Logger, config DeployManagerConfig, deployer Deployer, uuidProvider utils.UUIDProvider,
	deployRepository store.JobDeploymentRepository) *deployManager {
	mgr := &deployManager{
		requestQ:         make(chan models.DeployRequest),
		l:                l,
		config:           config,
		deployerCapacity: 0,
		deployer:         deployer,
		uuidProvider:     uuidProvider,
		deployRepository: deployRepository,
	}
	mgr.Init()
	return mgr
}
