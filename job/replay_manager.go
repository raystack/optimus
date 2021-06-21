package job

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/bus"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

var (
	// ErrRequestQueueFull signifies that the deployment manager's
	// request queue is full
	ErrRequestQueueFull = errors.New("request queue is full")
)

type ReplayManager interface {
	Init()
	Replay(models.ReplayRequestInput) (string, error)
}

// Manager for replaying operation(s).
// Offers an asynchronous interface to pipeline, with a fixed size request queue
// Only one replay happens at one time, any other request is queued, and executed
// when any in-progress operation is complete.
// The zero value of a Manager is an invalid Manager. Use `NewManager` constructor for
// creating a manager.
type Manager struct {
	// wait group to synchronise on workers
	wg sync.WaitGroup
	mu sync.Mutex

	// request queue, used by workers
	requestQ chan models.ReplayRequestInput
	// request map, used for verifying if a request is
	// in queue without actually consuming it
	requestMap map[uuid.UUID]bool

	//listen for replay requests inserted in db
	clearRequestMapListener chan interface{}

	//request worker
	replayWorker ReplayWorker
}

// Replay a request asynchronously, returns a replay id that can
// can be used to query its status
func (m *Manager) Replay(reqInput models.ReplayRequestInput) (string, error) {
	uuidOb, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	reqInput.ID = uuidOb

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

// start a worker goroutine that runs the deployment pipeline in background
func (m *Manager) spawnServiceWorker() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for reqInput := range m.requestQ {
			logger.I("worker picked up the request for ", reqInput.Project.Name)
			ctx := context.Background()

			if err := m.replayWorker.Process(ctx, reqInput); err != nil {
				//do something about this error
				logger.E(errors.Wrap(err, "worker failed to process"))
			}
		}
	}()
}

//Close stops consuming any new request
func (m *Manager) Close() error {
	if m.requestQ != nil {
		//stop accepting any more requests
		close(m.requestQ)
	}

	//wait for request worker to finish
	m.wg.Wait()

	_ = bus.Stop(EvtRecordInsertedInDB, m.clearRequestMapListener)
	_ = bus.Stop(EvtFailedToPrepareForReplay, m.clearRequestMapListener)
	if m.clearRequestMapListener != nil {
		close(m.clearRequestMapListener)
	}
	return nil
}

func (m *Manager) Init() {
	logger.I("starting replay workers")
	m.spawnServiceWorker()

	// listen for replay request being inserted in db
	bus.Listen(EvtRecordInsertedInDB, m.clearRequestMapListener)
	// listen when replay failed to even prepare to start
	bus.Listen(EvtFailedToPrepareForReplay, m.clearRequestMapListener)
	go func() {
		for {
			raw, ok := <-m.clearRequestMapListener
			if !ok {
				return
			}

			ID := raw.(uuid.UUID)
			m.mu.Lock()
			delete(m.requestMap, ID)
			m.mu.Unlock()
		}
	}()
}

// NewManager constructs a new instance of Manager
func NewManager(worker ReplayWorker, size int) *Manager {
	mgr := &Manager{
		replayWorker:            worker,
		requestMap:              make(map[uuid.UUID]bool),
		requestQ:                make(chan models.ReplayRequestInput, size),
		clearRequestMapListener: make(chan interface{}),
	}
	mgr.Init()
	return mgr
}
