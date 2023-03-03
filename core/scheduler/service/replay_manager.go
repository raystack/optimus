package service

import (
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/robfig/cron/v3"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/scheduler"
)

const (
	syncInterval = "@every 5m"
)

type ReplayManager struct {
	l log.Logger

	replayRepository ReplayRepository
	replayWorker     Worker

	schedule *cron.Cron
	Now      func() time.Time

	config config.ReplayConfig
}

type Worker interface {
	Process(context.Context, *scheduler.ReplayWithRun)
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

	// Cancel timed out replay with status [created, in progress, partial replayed, replayed]
	m.checkTimedOutReplay(ctx)

	// Fetch created, in progress, and replayed request
	replayToExecute, err := m.replayRepository.GetReplayToExecute(ctx)
	if err != nil {
		m.l.Error("unable to get replay requests to execute")
		return
	}

	go m.replayWorker.Process(ctx, replayToExecute)
}

func (m ReplayManager) checkTimedOutReplay(ctx context.Context) {
	onGoingReplays, err := m.replayRepository.GetReplayRequestsByStatus(ctx, []scheduler.ReplayState{scheduler.ReplayStateCreated,
		scheduler.ReplayStateInProgress, scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed})
	if err != nil {
		m.l.Error("unable to get on going replay")
	}

	for _, replay := range onGoingReplays {
		runningTime := m.Now().Sub(replay.CreatedAt())
		if runningTime < m.config.ReplayTimeout {
			continue
		}
		message := fmt.Sprintf("replay is timing out. %s", replay.Message())
		if err := m.replayRepository.UpdateReplayStatus(ctx, replay.ID(), scheduler.ReplayStateFailed, message); err != nil {
			m.l.Error("unable to mark replay %s as failed due to time out", replay.ID())
		}
		m.l.Info("replay %s is timing out. marked as failed.", replay.ID())
	}
}
