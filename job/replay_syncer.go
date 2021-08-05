package job

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
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
	replaySpecFactory         ReplaySpecRepoFactory
	scheduler                 models.SchedulerUnit
	dependencyResolver        DependencyResolver
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
	assetCompiler             AssetCompiler
	Now                       func() time.Time
	projectRepoFactory        ProjectRepoFactory
}

func NewReplaySyncer(replaySpecFactory ReplaySpecRepoFactory, scheduler models.SchedulerUnit,
	dependencyResolver DependencyResolver, projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	assetCompiler AssetCompiler, projectRepoFactory ProjectRepoFactory, timeFn func() time.Time) *Syncer {
	return &Syncer{
		replaySpecFactory:         replaySpecFactory,
		scheduler:                 scheduler,
		dependencyResolver:        dependencyResolver,
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
		assetCompiler:             assetCompiler,
		Now:                       timeFn,
		projectRepoFactory:        projectRepoFactory}
}

func (s Syncer) Sync(context context.Context, runTimeout time.Duration) error {
	replaySpecRepo := s.replaySpecFactory.New()
	replaySpecs, err := replaySpecRepo.GetByStatus(ReplayStatusToSynced)
	if err != nil {
		if err == store.ErrResourceNotFound {
			return nil
		}
		return err
	}

	for _, replaySpec := range replaySpecs {
		// sync end state of replayed replays
		if replaySpec.Status == models.ReplayStatusReplayed {
			if err := s.syncRunningReplay(context, replaySpec, replaySpecRepo); err != nil {
				return err
			}
			continue
		}

		// sync timed out replays for accepted and in progress replays
		if err := syncTimedOutReplay(replaySpecRepo, replaySpec, runTimeout); err != nil {
			return err
		}
	}
	return nil
}

func syncTimedOutReplay(replaySpecRepo store.ReplaySpecRepository, replaySpec models.ReplaySpec, runTimeout time.Duration) error {
	runningTime := time.Now().Sub(replaySpec.CreatedAt)
	if runningTime > runTimeout {
		if updateStatusErr := replaySpecRepo.UpdateStatus(replaySpec.ID, models.ReplayStatusFailed, models.ReplayMessage{
			Type:    ReplayRunTimeout,
			Message: fmt.Sprintf("replay has been running since %s", replaySpec.CreatedAt.UTC().Format(TimestampLogFormat)),
		}); updateStatusErr != nil {
			logger.I(fmt.Sprintf("marking long running replay jobs as failed: %s", updateStatusErr))
		}
	}
	return nil
}

func (s Syncer) syncRunningReplay(context context.Context, replaySpec models.ReplaySpec, replaySpecRepo store.ReplaySpecRepository) error {
	projSpec, err := s.projectRepoFactory.New().GetByName(replaySpec.Job.Project.Name)
	if err != nil {
		return err
	}
	jobSpecMap, err := s.prepareJobSpecMap(projSpec)
	if err != nil {
		return err
	}

	replayRequest := &models.ReplayRequest{
		ID:         replaySpec.ID,
		Job:        replaySpec.Job,
		Start:      replaySpec.StartDate,
		End:        replaySpec.EndDate,
		JobSpecMap: jobSpecMap,
	}

	//for each replay, will get its list of jobs and runs and check if completed
	treeNode, err := prepareReplayExecutionTree(replayRequest)
	if err != nil {
		return err
	}

	stateSummary, err := s.checkInstanceState(context, projSpec, treeNode.GetAllNodes(), replayRequest.Start, replayRequest.End)
	if err != nil {
		return err
	}

	return updateCompletedReplays(stateSummary, replaySpecRepo, replayRequest.ID)
}

func (s Syncer) prepareJobSpecMap(projSpec models.ProjectSpec) (map[string]models.JobSpec, error) {
	projectJobSpecRepo := s.projectJobSpecRepoFactory.New(projSpec)
	jobSpecs, err := projectJobSpecRepo.GetAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve jobs")
	}

	for i, jSpec := range jobSpecs {
		if jobSpecs[i].Assets, err = s.assetCompiler(jSpec, s.Now()); err != nil {
			return nil, errors.Wrap(err, "asset compilation")
		}
	}

	var resolvedSpecs []models.JobSpec
	for _, jobSpec := range jobSpecs {
		resolvedSpec, err := s.dependencyResolver.Resolve(projSpec, projectJobSpecRepo, jobSpec, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to resolve dependency for %s", jobSpec.Name)
		}
		resolvedSpecs = append(resolvedSpecs, resolvedSpec)
	}
	jobSpecMap := make(map[string]models.JobSpec)
	for _, currSpec := range resolvedSpecs {
		jobSpecMap[currSpec.Name] = currSpec
	}
	return jobSpecMap, nil
}

func (s Syncer) checkInstanceState(ctx context.Context, projSpec models.ProjectSpec, allNodes []*tree.TreeNode,
	startDate time.Time, endDate time.Time) (map[string]int, error) {
	stateSummary := make(map[string]int)
	stateSummary[models.InstanceStateRunning] = 0
	stateSummary[models.InstanceStateFailed] = 0
	stateSummary[models.InstanceStateSuccess] = 0

	for _, node := range allNodes {
		batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
		jobStatusAllRuns, err := s.scheduler.GetDagRunStatus(ctx, projSpec, node.GetName(), startDate, batchEndDate, schedulerBatchSize)
		if err != nil {
			return nil, err
		}
		for _, jobStatus := range jobStatusAllRuns {
			stateSummary[jobStatus.State.String()] = stateSummary[jobStatus.State.String()] + 1
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
