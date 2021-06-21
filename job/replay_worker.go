package job

import (
	"context"

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
)

type ReplayWorker interface {
	Process(context.Context, models.ReplayRequestInput) error
}

type replayWorker struct {
	replayRepo models.ReplayRepository
	scheduler  models.SchedulerUnit
}

func (w *replayWorker) Process(ctx context.Context, input models.ReplayRequestInput) (err error) {
	// save replay request
	replay := models.ReplaySpec{
		ID:        input.ID,
		Job:       input.Job,
		StartDate: input.Start,
		EndDate:   input.End,
		Status:    models.ReplayStatusAccepted,
		Project:   input.Project,
	}
	if err = w.replayRepo.Insert(&replay); err != nil {
		bus.Post(EvtFailedToPrepareForReplay, input.ID)
		return
	}

	replayTree, err := PrepareTree(input.DagSpecMap, input.Job.Name, input.Start, input.End)
	if err != nil {
		return err
	}

	replayDagsMap := make(map[string]*tree.TreeNode)
	replayTree.GetAllNodes(replayDagsMap)

	for jobName := range replayDagsMap {
		if err = w.scheduler.Clear(ctx, input.Project, jobName, input.Start, input.End); err != nil {
			return errors.Wrapf(err, "error while clearing dag runs for job %s", jobName)
		}
	}

	bus.Post(EvtRecordInsertedInDB, replay.ID)
	return nil
}

func NewReplayWorker(replayRepo models.ReplayRepository, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{replayRepo: replayRepo, scheduler: scheduler}
}
