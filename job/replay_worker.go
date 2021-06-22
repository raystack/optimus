package job

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/logger"

	"github.com/odpf/optimus/core/bus"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// EvtRecordInsertedInDB is emitted to event bus when a replay record is inserted in db
	// it passes replay ID as string in bus
	EvtRecordInsertedInDB = "replay_record_inserted_in_db"

	// EvtFailedToPrepareForReplay is emitted to event bus when a replay is failed to even prepare
	// to execute, it passes replay ID as string in bus
	EvtFailedToPrepareForReplay = "replay_request_failed_to_prepare"

	MsgReplaySuccessfullyCompleted = "Completed successfully"
	MsgReplayInProgress            = "Replay Request Picked up by replay worker"
)

type ReplayWorker interface {
	Process(context.Context, *models.ReplayRequestInput) error
}

type replayWorker struct {
	replaySpecRepoFac ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
}

func (w *replayWorker) Process(ctx context.Context, input *models.ReplayRequestInput) (err error) {
	replaySpecRepo := w.replaySpecRepoFac.New(input.Job)
	// mark replay request in progress
	inProgressErr := replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusInProgress, models.ReplayMessage{
		Status:  models.ReplayStatusInProgress,
		Message: MsgReplayInProgress,
	})
	if inProgressErr != nil {
		return inProgressErr
	}

	replayTree, err := prepareTree(input)
	if err != nil {
		return err
	}

	replayDagsMap := make(map[string]*tree.TreeNode)
	replayTree.GetAllNodes(replayDagsMap)

	for jobName, treeNode := range replayDagsMap {
		runTimes := treeNode.Runs.Values()
		startTime := runTimes[0].(time.Time)
		endTime := runTimes[treeNode.Runs.Size()-1].(time.Time)
		if err = w.scheduler.Clear(ctx, input.Project, jobName, startTime, endTime); err != nil {
			err = errors.Wrapf(err, "error while clearing dag runs for job %s", jobName)
			logger.W(fmt.Sprintf("error while running replay %s: %s", input.ID.String(), err.Error()))
			updateStatusErr := replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusFailed, models.ReplayMessage{
				Status:  models.ReplayStatusFailed,
				Message: err.Error(),
			})
			if updateStatusErr != nil {
				return updateStatusErr
			}
			return err
		}
	}

	err = replaySpecRepo.UpdateStatus(input.ID, models.ReplayStatusSuccess, models.ReplayMessage{
		Status:  models.ReplayStatusSuccess,
		Message: MsgReplaySuccessfullyCompleted,
	})
	if err != nil {
		return err
	}
	logger.I(fmt.Sprintf("successfully completed replay id: %s", input.ID.String()))
	bus.Post(EvtRecordInsertedInDB, input.ID)
	return nil
}

func NewReplayWorker(replaySpecRepoFac ReplaySpecRepoFactory, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{replaySpecRepoFac: replaySpecRepoFac, scheduler: scheduler}
}
