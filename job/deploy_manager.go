package job

import (
	"context"
	"errors"
	"fmt"
	"github.com/odpf/optimus/config"
	"sync"
	"sync/atomic"
	"time"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/utils"
)

type DeployManager interface {
	Init()
	Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error)
	GetStatus(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error)
}

type deployManager struct {
	l      log.Logger
	config config.Deployer

	deployer         Deployer
	deployerCapacity int32
	uuidProvider     utils.UUIDProvider
	deployRepository store.JobDeploymentRepository

	// wait group to synchronise on deployers
	wg sync.WaitGroup

	// request queue, used by deployers
	requestQ chan models.JobDeployment
}

func (m *deployManager) Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error) {
	// Check deployment status for the requested Project
	jobDeployment, err := m.deployRepository.GetByStatusAndProjectID(ctx, models.JobDeploymentStatusInQueue, projectSpec.ID)
	if err == nil {
		return jobDeployment.ID, nil
	}

	// Return valid errors
	if !errors.Is(err, store.ErrResourceNotFound) {
		return models.DeploymentID{}, err
	}

	newDeployment, err := m.createNewRequest(ctx, projectSpec)
	if err != nil {
		return newDeployment.ID, err
	}

	for {
		select {
		case m.requestQ <- newDeployment:
			m.l.Info(fmt.Sprintf("deployer taking request for %s", newDeployment.ID.UUID()))
			if err := m.updateDeploymentStatus(ctx, newDeployment, models.JobDeploymentStatusInQueue); err != nil {
				return models.DeploymentID{}, err
			}
			return newDeployment.ID, nil
		default:
			m.l.Info(fmt.Sprintf("failed to push deployment request ID %s to queue", newDeployment.ID.UUID()))
			if err := m.updateDeploymentStatus(ctx, newDeployment, models.JobDeploymentStatusCancelled); err != nil {
				return models.DeploymentID{}, err
			}
			return models.DeploymentID{}, errors.New("unable to push deployment request to queue")
		}
	}
}

func (m *deployManager) updateDeploymentStatus(ctx context.Context, jobDeployment models.JobDeployment, status models.JobDeploymentStatus) error {
	jobDeployment.Status = status
	return m.deployRepository.UpdateByID(ctx, jobDeployment)
}

func (m *deployManager) createNewRequest(ctx context.Context, projectSpec models.ProjectSpec) (models.JobDeployment, error) {
	newDeploymentID, err := m.uuidProvider.NewUUID()
	if err != nil {
		return models.JobDeployment{}, err
	}

	newDeployment := models.JobDeployment{
		ID:      models.DeploymentID(newDeploymentID),
		Project: projectSpec,
		Status:  models.JobDeploymentStatusCreated,
		Details: models.JobDeploymentDetail{},
	}

	if err := m.deployRepository.Save(ctx, newDeployment); err != nil {
		return models.JobDeployment{}, err
	}
	return newDeployment, nil
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

	for deployRequest := range m.requestQ {
		atomic.AddInt32(&m.deployerCapacity, -1)

		m.l.Info("deployer picked up the request", "request id", deployRequest)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := deployer.Deploy(ctx, deployRequest); err != nil {
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
func NewDeployManager(l log.Logger, deployerConfig config.Deployer, deployer Deployer, uuidProvider utils.UUIDProvider,
	deployRepository store.JobDeploymentRepository) *deployManager {
	mgr := &deployManager{
		requestQ:         make(chan models.JobDeployment, deployerConfig.QueueCapacity),
		l:                l,
		config:           deployerConfig,
		deployerCapacity: 0,
		deployer:         deployer,
		uuidProvider:     uuidProvider,
		deployRepository: deployRepository,
	}
	mgr.Init()
	return mgr
}
