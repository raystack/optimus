package job

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	AirflowClearDagRunFailed = "failed to clear airflow dag run"
)

type ReplayWorker interface {
	Process(context.Context, models.ReplayRequest) error
}

type replayWorker struct {
	replaySpecRepo store.ReplaySpecRepository
	scheduler      models.SchedulerUnit
	log            log.Logger
}

func (w *replayWorker) Process(ctx context.Context, input models.ReplayRequest) (err error) {
	// mark replay request in progress
	if inProgressErr := w.replaySpecRepo.UpdateStatus(ctx, input.ID, models.ReplayStatusInProgress, models.ReplayMessage{}); inProgressErr != nil {
		return inProgressErr
	}

	replaySpec, err := w.replaySpecRepo.GetByID(ctx, input.ID)
	if err != nil {
		return err
	}

	replayDagsMap := replaySpec.ExecutionTree.GetAllNodes()
	for _, treeNode := range replayDagsMap {
		runTimes := treeNode.Runs.Values()
		startTime := runTimes[0].(time.Time)
		endTime := runTimes[treeNode.Runs.Size()-1].(time.Time)
		if err = w.scheduler.Clear(ctx, input.Project, treeNode.GetName(), startTime, endTime); err != nil {
			err = fmt.Errorf("error while clearing dag runs for job %s: %w", treeNode.GetName(), err)
			w.log.Warn("error while running replay", "replay id", input.ID.String(), "error", err.Error())
			if updateStatusErr := w.replaySpecRepo.UpdateStatus(ctx, input.ID, models.ReplayStatusFailed, models.ReplayMessage{
				Type:    AirflowClearDagRunFailed,
				Message: err.Error(),
			}); updateStatusErr != nil {
				return updateStatusErr
			}
			return err
		}
	}

	if err := w.replaySpecRepo.UpdateStatus(ctx, input.ID, models.ReplayStatusReplayed, models.ReplayMessage{}); err != nil {
		return err
	}
	w.log.Info("successfully cleared instances during replay", "replay id", input.ID.String())
	return nil
}

func NewReplayWorker(l log.Logger, replaySpecRepo store.ReplaySpecRepository, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{
		log:            l,
		replaySpecRepo: replaySpecRepo,
		scheduler:      scheduler,
	}
}
