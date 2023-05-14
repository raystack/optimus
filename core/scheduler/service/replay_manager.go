package service

import (
	"time"

	"github.com/odpf/salt/log"
	"github.com/robfig/cron/v3"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/errors"
)

const (
	syncInterval = "@every 1m"
)

type ReplayManager struct {
	l log.Logger

	replayRepository ReplayRepository
	replayWorker     Worker

	schedule *cron.Cron
	Now      func() time.Time

	config config.ReplayConfig
}

func NewReplayManager(l log.Logger, replayRepository ReplayRepository, replayWorker Worker, now func() time.Time, config config.ReplayConfig) *ReplayManager {
	return &ReplayManager{
		l:                l,
		replayRepository: replayRepository,
		replayWorker:     replayWorker,
		Now:              now,
		config:           config,
		schedule: cron.New(cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		)),
	}
}

type Worker interface {
	Process(*scheduler.ReplayWithRun)
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
	ctx := context.Background()

	// Cancel timed out replay with status [created, in progress, partial replayed, replayed]
	m.checkTimedOutReplay(ctx)

	// Fetch created, in progress, and replayed request
	replayToExecute, err := m.replayRepository.GetReplayToExecute(ctx)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			m.l.Debug("no replay request found to execute")
		} else {
			m.l.Error("unable to get replay requests to execute: %w", err)
		}
		return
	}
	go m.replayWorker.Process(replayToExecute)
}

func (m ReplayManager) checkTimedOutReplay(ctx context.Context) {
	onGoingReplays, err := m.replayRepository.GetReplayRequestsByStatus(ctx, []scheduler.ReplayState{
		scheduler.ReplayStateCreated,
		scheduler.ReplayStateInProgress, scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed,
	})
	if err != nil {
		m.l.Error("unable to get on going replay")
	}

	for _, replay := range onGoingReplays {
		runningTime := m.Now().Sub(replay.CreatedAt())
		if runningTime < m.config.ReplayTimeout {
			continue
		}
		message := "replay timed out"
		if err := m.replayRepository.UpdateReplayStatus(ctx, replay.ID(), scheduler.ReplayStateFailed, message); err != nil {
			m.l.Error("unable to mark replay %s as failed due to time out", replay.ID())
		}
		m.l.Info("replay %s timed out. marked as failed.", replay.ID())
	}
}
