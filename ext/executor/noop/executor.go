package noop

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/odpf/optimus/models"
)

type Executor struct {
	state map[string]models.JobRunState
	mu    *sync.Mutex
}

func NewExecutor() *Executor {
	return &Executor{
		state: map[string]models.JobRunState{},
		mu:    new(sync.Mutex),
	}
}

func (e *Executor) Start(ctx context.Context, req *models.ExecutorStartRequest) (*models.ExecutorStartResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state[req.ID] = models.RunStateRunning
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

func (e *Executor) WaitForFinish(ctx context.Context, id string) (chan int, error) {
	if _, ok := e.state[id]; !ok {
		return nil, errors.New("invalid id, no such execution")
	}

	resultChan := make(chan int)
	go func(id string) {
		// simulate work
		time.Sleep(time.Second * 5)

		// mark complete
		e.mu.Lock()
		defer e.mu.Unlock()
		e.state[id] = models.RunStateSuccess

		resultChan <- 0 // exit code 0
		close(resultChan)
	}(id)
	return resultChan, nil
}

func (e *Executor) Stats(ctx context.Context, id string) (*models.ExecutorStats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.state[id]; !ok {
		return nil, errors.New("invalid id, no such execution")
	}

	return &models.ExecutorStats{
		Status: e.state[id].String(),
	}, nil
}
