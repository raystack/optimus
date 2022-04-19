package job

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/dag"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	// ReplayStatusToValidate signifies list of status to be used when checking active replays
	ReplayStatusToValidate = []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted, models.ReplayStatusReplayed}
)

type Validator struct {
	scheduler models.SchedulerUnit
}

func NewReplayValidator(scheduler models.SchedulerUnit) *Validator {
	return &Validator{scheduler: scheduler}
}

func (v *Validator) Validate(ctx context.Context, replaySpecRepo store.ReplaySpecRepository,
	reqInput models.ReplayRequest, replayTree *dag.TreeNode) error {
	if !reqInput.Force {
		reqReplayNodes := replayTree.GetAllNodes()

		//check if this dag have running instance in the batchScheduler
		if err := v.validateRunningInstance(ctx, reqReplayNodes, reqInput); err != nil {
			return err
		}

		//check another replay active for this dag
		activeReplaySpecs, err := replaySpecRepo.GetByStatus(ctx, ReplayStatusToValidate)
		if err != nil {
			if err == store.ErrResourceNotFound {
				return nil
			}
			return err
		}
		return validateReplayJobsConflict(activeReplaySpecs, reqReplayNodes)
	}
	//check and cancel if found conflicted replays for same job ID
	return cancelConflictedReplays(ctx, replaySpecRepo, reqInput)
}

func cancelConflictedReplays(ctx context.Context, replaySpecRepo store.ReplaySpecRepository, reqInput models.ReplayRequest) error {
	duplicatedReplaySpecs, err := replaySpecRepo.GetByJobIDAndStatus(ctx, reqInput.Job.ID, ReplayStatusToValidate)
	if err != nil {
		if err == store.ErrResourceNotFound {
			return nil
		}
		return err
	}
	for _, replaySpec := range duplicatedReplaySpecs {
		if err := replaySpecRepo.UpdateStatus(ctx, replaySpec.ID, models.ReplayStatusCancelled, models.ReplayMessage{
			Type:    ErrConflictedJobRun.Error(),
			Message: fmt.Sprintf("force started replay with ID: %s", reqInput.ID),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validateRunningInstance(ctx context.Context, reqReplayNodes []*dag.TreeNode, reqInput models.ReplayRequest) error {
	for _, reqReplayNode := range reqReplayNodes {
		batchEndDate := reqInput.End.AddDate(0, 0, 1).Add(time.Second * -1)
		jobStatusAllRuns, err := v.scheduler.GetJobRunStatus(ctx, reqInput.Project, reqReplayNode.Data.(models.JobSpec).Name, reqInput.Start, batchEndDate, schedulerBatchSize)
		if err != nil {
			return err
		}
		for _, jobStatus := range jobStatusAllRuns {
			if reqReplayNode.Runs.Contains(jobStatus.ScheduledAt) && jobStatus.State == models.RunStateRunning {
				return ErrConflictedJobRun
			}
		}
	}
	return nil
}

func validateReplayJobsConflict(activeReplaySpecs []models.ReplaySpec, reqReplayNodes []*dag.TreeNode) error {
	for _, activeSpec := range activeReplaySpecs {
		activeNodes := activeSpec.ExecutionTree.GetAllNodes()
		if err := checkAnyConflictedDags(activeNodes, reqReplayNodes); err != nil {
			return err
		}
	}
	return nil
}

func checkAnyConflictedDags(activeNodes []*dag.TreeNode, reqReplayNodes []*dag.TreeNode) error {
	activeNodesMap := make(map[string]*dag.TreeNode)
	for _, activeNode := range activeNodes {
		activeNodesMap[activeNode.String()] = activeNode
	}

	for _, reqNode := range reqReplayNodes {
		if _, ok := activeNodesMap[reqNode.String()]; ok {
			if err := checkAnyConflictedRuns(activeNodesMap[reqNode.String()], reqNode); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkAnyConflictedRuns(activeNode *dag.TreeNode, reqNode *dag.TreeNode) error {
	for _, reqNodeRun := range reqNode.Runs.Values() {
		if activeNode.Runs.Contains(reqNodeRun.(time.Time)) {
			return ErrConflictedJobRun
		}
	}
	return nil
}
