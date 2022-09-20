package job

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/odpf/salt/log"
	"github.com/robfig/cron/v3"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	deployAssignInterval = "@every 1m"
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

	// unbuffered request channel to assign request to deployer
	requestQ chan models.JobDeployment

	assignerScheduler *cron.Cron
}

func (m *deployManager) Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error) {
	// Check deployment status for the requested Project
	jobDeployment, err := m.deployRepository.GetByStatusAndProjectID(ctx, models.JobDeploymentStatusInQueue, projectSpec.ID)
	if err == nil {
		m.l.Info("deployment request already exist in queue", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)
		return jobDeployment.ID, nil
	}

	// Return valid errors
	if !errors.Is(err, store.ErrResourceNotFound) {
		return models.DeploymentID{}, err
	}

	newDeployment, err := m.createNewRequest(ctx, projectSpec)
	return newDeployment.ID, err
}

func (m *deployManager) createNewRequest(ctx context.Context, projectSpec models.ProjectSpec) (models.JobDeployment, error) {
	newDeploymentID, err := m.uuidProvider.NewUUID()
	if err != nil {
		return models.JobDeployment{}, err
	}

	newDeployment := models.JobDeployment{
		ID:      models.DeploymentID(newDeploymentID),
		Project: projectSpec,
		Status:  models.JobDeploymentStatusInQueue,
		Details: models.JobDeploymentDetail{},
	}

	if err := m.deployRepository.Save(ctx, newDeployment); err != nil {
		return models.JobDeployment{}, err
	}
	m.l.Info("new deployment request created", "request id", newDeployment.ID.UUID(), "project name", newDeployment.Project.Name)
	return newDeployment, nil
}

func (m *deployManager) GetStatus(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	return m.deployRepository.GetByID(ctx, deployID)
}

func (m *deployManager) Init() {
	if m.assignerScheduler != nil {
		_, err := m.assignerScheduler.AddFunc(deployAssignInterval, m.Assign)
		if err != nil {
			m.l.Fatal("failed to schedule deployment assigner", "error", err.Error())
		}
		m.assignerScheduler.Start()
	}
}

// start a deployer goroutine that runs the deployment in background
func (m *deployManager) spawnDeployer(deployer Deployer, deployRequest models.JobDeployment) {
	// deployer has spawned
	defer atomic.AddInt32(&m.deployerCapacity, 1)

	m.l.Info("deployer start processing a request", "request id", deployRequest.ID.UUID(), "project name", deployRequest.Project.Name)
	ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
	defer cancelCtx()
	if err := deployer.Deploy(ctx, deployRequest); err != nil {
		m.l.Error("deployer failed to process", deployRequest.ID.UUID(), "project name", deployRequest.Project.Name, "error", err.Error())
		deployRequest.Status = models.JobDeploymentStatusFailed
		deployRequest.Details.Failures = []models.JobDeploymentFailure{{Message: err.Error()}}
		err := m.deployRepository.Update(ctx, deployRequest)
		if err != nil {
			m.l.Error("unable to mark deployment request as cancelled", deployRequest.ID.UUID(), "project name", deployRequest.Project.Name, "error", err.Error())
		}
	}
}

func (m *deployManager) Assign() {
	ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
	defer cancelCtx()
	m.cancelTimedOutDeployments(ctx)

	if int(atomic.LoadInt32(&m.deployerCapacity)) <= 0 {
		m.l.Info("deployers are all occupied.")
		return
	}

	limit := int(atomic.LoadInt32(&m.deployerCapacity))
	jobDeployments, err := m.deployRepository.GetAndUpdateExecutableRequests(ctx, limit)
	if err != nil {
		m.l.Error("failed to fetch executable deployment request to assign deployer", "error", err.Error())
		return
	}
	if len(jobDeployments) == 0 {
		m.l.Debug("no deployment request found to assign deployer")
		return
	}
	for _, jobDeployment := range jobDeployments {
		m.l.Info("deployer taking a request", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)
		atomic.AddInt32(&m.deployerCapacity, -1)
		go m.spawnDeployer(m.deployer, jobDeployment)
	}
}

func (m *deployManager) cancelTimedOutDeployments(ctx context.Context) {
	inProgressDeployments, err := m.deployRepository.GetByStatus(ctx, models.JobDeploymentStatusInProgress)
	if err != nil {
		m.l.Error("failed to fetch in progress deployments", "error", err.Error())
		return
	}

	for _, deployment := range inProgressDeployments {
		m.l.Info(fmt.Sprintf("%s job deployment is in progress.", deployment.Project.Name), "time taken", time.Since(deployment.UpdatedAt).Round(time.Second))

		// check state / timed out deployment, mark as cancelled
		if time.Since(deployment.UpdatedAt).Minutes() > m.config.WorkerTimeout.Minutes() {
			deployment.Status = models.JobDeploymentStatusCancelled
			if err := m.deployRepository.Update(ctx, deployment); err != nil {
				m.l.Error("failed to mark timed out deployment as cancelled", "project name", deployment.Project.Name, "error", err.Error())
			} else {
				m.l.Info("marked timed out deployment as cancelled", "project name", deployment.Project.Name)
			}
		}
	}
}

// NewDeployManager constructs a new instance of Job Deployment Manager
func NewDeployManager(l log.Logger, deployerConfig config.Deployer, deployer Deployer, uuidProvider utils.UUIDProvider,
	deployRepository store.JobDeploymentRepository, assignerScheduler *cron.Cron) *deployManager {
	mgr := &deployManager{
		requestQ:          make(chan models.JobDeployment),
		l:                 l,
		config:            deployerConfig,
		deployerCapacity:  int32(deployerConfig.NumWorkers),
		deployer:          deployer,
		uuidProvider:      uuidProvider,
		deployRepository:  deployRepository,
		assignerScheduler: assignerScheduler,
	}
	mgr.Init()
	return mgr
}
