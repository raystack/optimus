package noop

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/odpf/optimus/models"
)

type Executor struct {
	state     map[string]models.ExecutorStatus
	mu        *sync.Mutex
	fakeSleep time.Duration
}

func NewExecutor() *Executor {
	return &Executor{
		state:     map[string]models.ExecutorStatus{},
		mu:        new(sync.Mutex),
		fakeSleep: time.Second * 2,
	}
}

func (e *Executor) Start(ctx context.Context, req *models.ExecutorStartRequest) (*models.ExecutorStartResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state[req.InstanceID] = models.ExecutorStatusRunning
	return &models.ExecutorStartResponse{}, nil
}

func (e *Executor) Stop(ctx context.Context, req *models.ExecutorStopRequest) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.state[req.ID]; !ok {
		return nil
	}
	delete(e.state, req.ID)
	return nil
}

func (e *Executor) WaitForFinish(ctx context.Context, id string) (int, error) {
	if _, ok := e.state[id]; !ok {
		return 1, errors.New("invalid id, no such execution")
	}
	time.Sleep(e.fakeSleep)

	// mark complete
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state[id] = models.ExecutorStatusSuccess
	return 0, nil
}

func (e *Executor) Stats(ctx context.Context, id string) (*models.ExecutorStats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.state[id]; !ok {
		return nil, errors.New("invalid id, no such execution")
	}

	return &models.ExecutorStats{
		Status: e.state[id],
	}, nil
}
