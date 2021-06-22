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
)

type ReplayWorker interface {
	Process(context.Context, *models.ReplayRequestInput) error
}

type replayWorker struct {
	replayRepo models.ReplayRepository
	scheduler  models.SchedulerUnit
}

func (w *replayWorker) Process(ctx context.Context, input *models.ReplayRequestInput) (err error) {
	// save replay request
	replay := models.ReplaySpec{
		ID:        input.ID,
		Job:       input.Job,
		StartDate: input.Start,
		EndDate:   input.End,
		Status:    models.ReplayStatusAccepted,
	}
	if err = w.replayRepo.Insert(&replay); err != nil {
		bus.Post(EvtFailedToPrepareForReplay, input.ID)
		return
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
			logger.W(fmt.Sprintf("error while running replay %s: %s", replay.ID.String(), err.Error()))
			err = w.replayRepo.UpdateStatus(replay.ID, models.ReplayStatusFailed, err.Error())
			if err != nil {
				return err
			}
			return err
		}
	}

	err = w.replayRepo.UpdateStatus(replay.ID, models.ReplayStatusSuccess, MsgReplaySuccessfullyCompleted)
	if err != nil {
		return err
	}
	logger.I(fmt.Sprintf("successfully completed replay id: %s", replay.ID.String()))
	bus.Post(EvtRecordInsertedInDB, replay.ID)
	return nil
}

func NewReplayWorker(replayRepo models.ReplayRepository, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{replayRepo: replayRepo, scheduler: scheduler}
}
