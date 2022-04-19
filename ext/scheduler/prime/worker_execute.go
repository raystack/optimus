package prime

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/gossip"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/models"
)

// nodeJobExecution looks for job assigned to this node and executes them
// this is executed by leaders as well as workers because each leader is also
// a worker.
func (p *Planner) nodeJobExecution(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	loopIdx := 0
	for {
		runRepo := p.jobRunRepoFac.New()

		// find what we need to execute
		localNodeID := p.clusterManager.GetLocalMember().Name
		currentAllocations, ok := p.clusterManager.GetState().Allocation[localNodeID]
		if ok {
			for _, rawAlloc := range currentAllocations.Values() {
				alloc := rawAlloc.(gossip.StateJob)
				if alloc.Status == models.RunStateAccepted.String() ||
					alloc.Status == models.RunStateRunning.String() {
					// execute this
					runUUID, err := uuid.Parse(alloc.UUID)
					if err != nil {
						p.errChan <- err
						return
					}
					jobRun, namespaceSpec, err := runRepo.GetByID(ctx, runUUID)
					if err != nil {
						p.errChan <- err
						return
					}

					if err := p.executeRun(ctx, namespaceSpec, jobRun); err != nil {
						p.errChan <- err
						return
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			// repeats loop:
			loopIdx++
		}
	}
}

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

	// TODO(kushsharma): parallelize execution for each root node
	for _, root := range execGraph.GetRootNodes() {
		var execOrder []*models.ExecutorStartRequest
		for _, item := range execGraph.ReverseTopologicalSort(root) {
			execOrder = append(execOrder, item.Data.(ExecNode).GetRequest())
		}

		for _, currentExec := range execOrder {
			for _, instance := range jobRun.Instances {
				if currentExec.Unit.Info().Name == instance.Name && instance.Status == models.RunStateSuccess {
					continue
				}
			}
			if err := p.executeInstance(ctx, jobRun.ID, currentExec); err != nil {
				err = multierror.Append(err, err)
				// stop this exec sequence but we can try to execute parallel dags
				// from a different root
				break
			}
		}
	}
	return err
}

func (p *Planner) executeInstance(ctx context.Context, jobRunID uuid.UUID, startRequest *models.ExecutorStartRequest) error {
	instanceName := startRequest.Unit.Info().Name
	instanceID, err := uuid.Parse(startRequest.InstanceID)
	if err != nil {
		return fmt.Errorf("failed to parse uuid for instance: %v(%s)", err, instanceName)
	}

	// create an instance, if already present, replace it as fresh
	if err := p.broadcastInstanceCreate(GossipInstanceCreateRequest{
		RunID:      jobRunID,
		InstanceID: instanceID,
		Name:       instanceName,
		Type:       startRequest.Type,
	}); err != nil {
		return err
	}

	// send it to executor for execution
	p.l.Info("starting executing job", "instance name", instanceName)
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
	exitCode, err := p.executor.WaitForFinish(ctx, instanceID.String())
	if err != nil {
		return err
	}
	if exitCode == 0 {
		// mark instance success
		if err := p.broadcastInstanceStatus(GossipInstanceUpdateRequest{
			InstanceID: instanceID,
			RunState:   models.RunStateSuccess,
		}); err != nil {
			return err
		}
	} else {
		p.l.Warn("job finished with non zero code", "code", exitCode, "instance name", instanceName)

		// mark instance failed
		if err := p.broadcastInstanceStatus(GossipInstanceUpdateRequest{
			InstanceID: instanceID,
			RunState:   models.RunStateFailed,
		}); err != nil {
			return err
		}

		instanceStatus, err := p.executor.Stats(ctx, instanceID.String())
		if err != nil {
			return err
		}
		p.l.Info("logs for failed execution", "instance id", instanceID,
			"status", instanceStatus.Status, "exit code", instanceStatus.ExitCode,
			"logs", string(instanceStatus.Logs))
	}
	p.l.Info("finished executing job spec", "instance name", instanceName)
	return nil
}
