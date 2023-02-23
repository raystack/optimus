package service

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/salt/log"
	"github.com/robfig/cron/v3"
	"golang.org/x/net/context"
)

const (
	syncInterval = "@every 5m"
)

type ReplayManager struct {
	l log.Logger

	replayRepository ReplayRepository
	replayWorker     Worker

	schedule *cron.Cron

	config config.ReplayConfig
}

type Worker interface {
	Process(context.Context, []*scheduler.StoredReplay)
}

func (m ReplayManager) Initialize() {
	if m.schedule != nil {
		_, err := m.schedule.AddFunc(syncInterval, m.StartReplayLoop)
		if err != nil {
			m.l.Error("Failed to sync replay", "error", err)
		}
		m.schedule.Start()
	}
}

func (m ReplayManager) StartReplayLoop() {
	ctx, cancelCtx := context.WithTimeout(context.Background(), m.config.WorkerTimeout)
	defer cancelCtx()

	// Cancel timed out replay with status [created, in progress, replayed]

	// Fetch created, in progress, and replayed requests
	replaysToExecute, err := m.replayRepository.GetReplaysToExecute(ctx)
	if err != nil {
		m.l.Error("unable to get replay requests to execute")
		return
	}

	go m.replayWorker.Process(ctx, replaysToExecute)
}
