package noop

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/odpf/optimus/models"
)

const MockTaskDuration = time.Second * 5

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

func (e *Executor) Start(_ context.Context, req models.ExecutorStartRequest) *models.ExecutorStartResponse {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state[req.ID] = models.RunStateRunning
	return &models.ExecutorStartResponse{}
}

func (e *Executor) Stop(_ context.Context, req models.ExecutorStopRequest) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.state[req.ID]; !ok {
		return errors.New("invalid id, no such execution")
	}

	delete(e.state, req.ID)
	return nil
}

func (e *Executor) WaitForFinish(_ context.Context, id string) (chan int, error) {
	if _, ok := e.state[id]; !ok {
		return nil, errors.New("invalid id, no such execution")
	}

	resultChan := make(chan int)
	go func(id string) {
		// simulate work
		time.Sleep(MockTaskDuration)

		// mark complete
		e.mu.Lock()
		defer e.mu.Unlock()
		e.state[id] = models.RunStateSuccess

		resultChan <- 0 // exit code 0
		close(resultChan)
	}(id)
	return resultChan, nil
}

func (e *Executor) Stats(_ context.Context, id string) (*models.ExecutorStats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.state[id]; !ok {
		return nil, errors.New("invalid id, no such execution")
	}

	return &models.ExecutorStats{
		Status: e.state[id].String(),
	}, nil
}
