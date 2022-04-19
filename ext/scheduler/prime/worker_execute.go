package prime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/models"
)

// executeRun finds all tasks/hooks that belong to this run job spec and
// execute them in order.
// As each context gets executed, its state should be updated in job run
// instance store.
// TODO(kushsharma): Currently each node is executing in blocking state,
// that is only one thing can execute at a time in a node, but this
// should be made async.
// For each exec instance in job
// - check if job run instances(tasks/hooks) are already in success state
// - if not find which one is not and create/execute them
func (p *Planner) executeRun(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun) error {
	var err error
	execGraph, err := BuildGraph(p.uuidProvider, namespace, jobRun.Spec)
	if err != nil {
		return err
	}

	for _, root := range execGraph.GetRootNodes() {
		var execOrder []*models.ExecutorStartRequest
		for _, item := range execGraph.ReverseTopologicalSort(root) {
			// TODO(kushsharma): parallelize execution for each root node
			execOrder = append(execOrder, item.Data.(ExecNode).GetRequest())
		}

		for _, currentExec := range execOrder {
			for _, instance := range jobRun.Instances {
				if currentExec.Name == instance.Name && instance.Status == models.RunStateSuccess {
					continue
				}
			}
			err := p.executeInstance(ctx, currentExec, jobRun)
			if err != nil {
				err = multierror.Append(err, err)
				// stop this exec sequence but we can try to execute parallel dags
				break
			}
		}
	}
	return err
}

func (p *Planner) executeInstance(ctx context.Context, startRequest *models.ExecutorStartRequest, jobRun models.JobRun) error {
	instanceID, err := uuid.Parse(startRequest.ID)
	if err != nil {
		return fmt.Errorf("failed to parse uuid for instance: %v", err)
	}

	// create an instance, if already present, replace it as fresh
	if err := p.broadcastInstanceCreate(GossipInstanceCreateRequest{
		RunID:      jobRun.ID,
		InstanceID: instanceID,
		Name:       startRequest.Name,
		Type:       startRequest.Type,
	}); err != nil {
		return err
	}

	// send it to executor for execution
	p.l.Info("starting executing job", "instance name", startRequest.Name)
	_, err = p.executor.Start(ctx, startRequest)
	if err != nil {
		return err
	}

	// mark instance running
	if err := p.broadcastInstanceStatus(GossipInstanceUpdateRequest{
		InstanceID: instanceID,
		RunState:   models.RunStateRunning,
	}); err != nil {
		return err
	}

	// block until the given task finishes
	finishChan, err := p.executor.WaitForFinish(ctx, instanceID.String())
	if err != nil {
		return err
	}
	if finishCode := <-finishChan; finishCode != 0 {
		p.l.Warn("job finished with non zero code", "code", finishCode, "instance name", startRequest.Name)

		// mark instance failed
		if err := p.broadcastInstanceStatus(GossipInstanceUpdateRequest{
			InstanceID: instanceID,
			RunState:   models.RunStateFailed,
		}); err != nil {
			return err
		}
	}

	// mark instance success
	if err := p.broadcastInstanceStatus(GossipInstanceUpdateRequest{
		InstanceID: instanceID,
		RunState:   models.RunStateSuccess,
	}); err != nil {
		return err
	}
	p.l.Info("finished executing job spec", "instance name", startRequest.Name)
	return nil
}
