package job

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	ReplayStatusToSynced = []string{models.ReplayStatusReplayed, models.ReplayStatusInProgress, models.ReplayStatusAccepted}
	ReplayMessageSuccess = "all instances for this replay are successfully run"
	ReplayMessageFailed  = "instance run failure found"
)

type ReplaySyncer interface {
	Sync(context.Context, time.Duration) error
}

type Syncer struct {
	replaySpecFactory  ReplaySpecRepoFactory
	projectRepoFactory ProjectRepoFactory
	scheduler          models.SchedulerUnit
	Now                func() time.Time
}

func NewReplaySyncer(replaySpecFactory ReplaySpecRepoFactory, projectRepoFactory ProjectRepoFactory, scheduler models.SchedulerUnit,
	timeFn func() time.Time) *Syncer {
	return &Syncer{
		replaySpecFactory:  replaySpecFactory,
		projectRepoFactory: projectRepoFactory,
		scheduler:          scheduler,
		Now:                timeFn,
	}
}

func (s Syncer) Sync(context context.Context, runTimeout time.Duration) error {
	replaySpecRepo := s.replaySpecFactory.New()

	projectSpecs, err := s.projectRepoFactory.New().GetAll()
	if err != nil {
		return err
	}
	for _, projectSpec := range projectSpecs {
		replaySpecs, err := replaySpecRepo.GetByProjectIDAndStatus(projectSpec.ID, ReplayStatusToSynced)
		if err != nil {
			if err == store.ErrResourceNotFound {
				return nil
			}
			return err
		}

		for _, replaySpec := range replaySpecs {
			// sync end state of replayed replays
			if replaySpec.Status == models.ReplayStatusReplayed {
				if err := s.syncRunningReplay(context, projectSpec, replaySpec, replaySpecRepo); err != nil {
					return err
				}
				continue
			}

			// sync timed out replays for accepted and in progress replays
			if err := s.syncTimedOutReplay(replaySpecRepo, replaySpec, runTimeout); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s Syncer) syncTimedOutReplay(replaySpecRepo store.ReplaySpecRepository, replaySpec models.ReplaySpec, runTimeout time.Duration) error {
	runningTime := s.Now().Sub(replaySpec.CreatedAt)
	if runningTime > runTimeout {
		if updateStatusErr := replaySpecRepo.UpdateStatus(replaySpec.ID, models.ReplayStatusFailed, models.ReplayMessage{
			Type:    ReplayRunTimeout,
			Message: fmt.Sprintf("replay has been running since %s", replaySpec.CreatedAt.UTC().Format(TimestampLogFormat)),
		}); updateStatusErr != nil {
			logger.I(fmt.Sprintf("marking long running replay jobs as failed: %s", updateStatusErr))
			return updateStatusErr
		}
	}
	return nil
}

func (s Syncer) syncRunningReplay(context context.Context, projectSpec models.ProjectSpec, replaySpec models.ReplaySpec, replaySpecRepo store.ReplaySpecRepository) error {
	stateSummary, err := s.checkInstanceState(context, projectSpec, replaySpec)
	if err != nil {
		return err
	}

	return updateCompletedReplays(stateSummary, replaySpecRepo, replaySpec.ID)
}

func (s Syncer) checkInstanceState(ctx context.Context, projectSpec models.ProjectSpec, replaySpec models.ReplaySpec) (map[string]int, error) {
	stateSummary := make(map[string]int)
	stateSummary[models.InstanceStateRunning] = 0
	stateSummary[models.InstanceStateFailed] = 0
	stateSummary[models.InstanceStateSuccess] = 0

	for _, node := range replaySpec.ExecutionTree.GetAllNodes() {
		batchEndDate := replaySpec.EndDate.AddDate(0, 0, 1).Add(time.Second * -1)
		jobStatusAllRuns, err := s.scheduler.GetDagRunStatus(ctx, projectSpec, node.Data.(models.JobSpec).Name, replaySpec.StartDate, batchEndDate, schedulerBatchSize)
		if err != nil {
			return nil, err
		}
		for _, jobStatus := range jobStatusAllRuns {
			stateSummary[jobStatus.State.String()]++
		}
	}
	return stateSummary, nil
}

func updateCompletedReplays(stateSummary map[string]int, replaySpecRepo store.ReplaySpecRepository, replayID uuid.UUID) error {
	if stateSummary[models.InstanceStateRunning] == 0 && stateSummary[models.InstanceStateFailed] > 0 {
		if updateStatusErr := replaySpecRepo.UpdateStatus(replayID, models.ReplayStatusFailed, models.ReplayMessage{
			Type:    models.ReplayStatusFailed,
			Message: ReplayMessageFailed,
		}); updateStatusErr != nil {
			logger.I(fmt.Sprintf("marking replay as failed error: %s", updateStatusErr))
			return updateStatusErr
		}
	} else if stateSummary[models.InstanceStateRunning] == 0 && stateSummary[models.InstanceStateFailed] == 0 && stateSummary[models.InstanceStateSuccess] > 0 {
		if updateStatusErr := replaySpecRepo.UpdateStatus(replayID, models.ReplayStatusSuccess, models.ReplayMessage{
			Type:    models.ReplayStatusSuccess,
			Message: ReplayMessageSuccess,
		}); updateStatusErr != nil {
			logger.I(fmt.Sprintf("marking replay as success error: %s", updateStatusErr))
			return updateStatusErr
		}
	}
	return nil
}
