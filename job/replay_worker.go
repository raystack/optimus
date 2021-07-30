package job

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/logger"

	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	AirflowClearDagRunFailed = "failed to clear airflow dag run"
)

type ReplayWorker interface {
	Process(context.Context, *models.ReplayRequest) error
}

type replayWorker struct {
	replaySpecRepoFac ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
}

func (w *replayWorker) Process(ctx context.Context, input *models.ReplayRequest) (err error) {
	replaySpecRepo := w.replaySpecRepoFac.New(input.Job)
	// mark replay request in progress
	if inProgressErr := replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusInProgress, models.ReplayMessage{}); inProgressErr != nil {
		return inProgressErr
	}

	replayTree, err := prepareReplayExecutionTree(input)
	if err != nil {
		return err
	}

	replayDagsMap := replayTree.GetAllNodes()
	for _, treeNode := range replayDagsMap {
		runTimes := treeNode.Runs.Values()
		startTime := runTimes[0].(time.Time)
		endTime := runTimes[treeNode.Runs.Size()-1].(time.Time)
		if err = w.scheduler.Clear(ctx, input.Project, treeNode.GetName(), startTime, endTime); err != nil {
			err = errors.Wrapf(err, "error while clearing dag runs for job %s", treeNode.GetName())
			logger.W(fmt.Sprintf("error while running replay %s: %s", input.ID.String(), err.Error()))
			if updateStatusErr := replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusFailed, models.ReplayMessage{
				Type:    AirflowClearDagRunFailed,
				Message: err.Error(),
			}); updateStatusErr != nil {
				return updateStatusErr
			}
			return err
		}
	}

	if err = replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusReplayed, models.ReplayMessage{}); err != nil {
		return err
	}
	logger.I(fmt.Sprintf("successfully cleared instances of replay id: %s", input.ID.String()))
	return nil
}

func NewReplayWorker(replaySpecRepoFac ReplaySpecRepoFactory, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{replaySpecRepoFac: replaySpecRepoFac, scheduler: scheduler}
}
