package job

import (
	"context"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"sync"
	"sync/atomic"
	"time"
)

type DeployManager struct {
	// wait group to synchronise on workers
	wg sync.WaitGroup

	// request queue, used by workers
	requestQ chan models.ProjectSpec

	l log.Logger

	config DeployManagerConfig

	deployerCapacity int32
	deployerFactory  DeployerFactory
}

type DeployManagerConfig struct {
	NumWorkers    int
	WorkerTimeout time.Duration
	//RunTimeout    time.Duration
}

type DeployerFactory interface {
	New() Deployer
}

func (m *DeployManager) Deploy() {
	//
}

func (m *DeployManager) Init() {
	m.l.Info("starting deployers", "count", m.config.NumWorkers)
	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		worker := m.deployerFactory.New()
		go m.spawnServiceWorker(worker)
	}

	// wait until all workers are ready
	for {
		if int(atomic.LoadInt32(&m.deployerCapacity)) == m.config.NumWorkers {
			break
		}
		time.Sleep(time.Millisecond * 50) //nolint: gomnd
	}
}

// start a worker goroutine that runs the deployment in background
func (m *DeployManager) spawnServiceWorker(deployer Deployer) {
	defer m.wg.Done()
	atomic.AddInt32(&m.deployerCapacity, 1)
	for reqInput := range m.requestQ {
		atomic.AddInt32(&m.deployerCapacity, -1)

		m.l.Info("deployment worker picked up the request", "request id", reqInput.ID)
		ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
		if err := deployer.Deploy(ctx, reqInput); err != nil {
			m.l.Error("deployment worker failed to process", "error", err)
		}
		cancelCtx()

		atomic.AddInt32(&m.deployerCapacity, 1)
	}
}

// Close stops consuming any new request
func (m *DeployManager) Close() error { // nolint: unparam
	if m.requestQ != nil {
		// stop accepting any more requests
		close(m.requestQ)
	}
	// wait for request worker to finish
	m.wg.Wait()

	return nil
}
