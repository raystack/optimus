package prime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/cluster/v1beta1"
	"github.com/odpf/optimus/core/gossip"
	"github.com/odpf/optimus/models"
	"google.golang.org/protobuf/proto"
)

// leaderJobAllocation allocates the user requested jobs to cluster
// nodes for execution.
// Requested jobs are pulled from database and once assigned whom
// to execute what, this status is also persisted in database.
func (p *Planner) leaderJobAllocation(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	loopIdx := 0
	for {
		if !p.clusterManager.IsLeader() {
			time.Sleep(NonLeaderSleepTime)
			continue
		}

		allocNodeID, allocRunIDs, err := p.getJobAllocations(ctx)
		if err != nil {
			p.errChan <- err
			return
		}
		if len(allocRunIDs) > 0 {
			var stringRunIDs []string
			for _, ri := range allocRunIDs {
				stringRunIDs = append(stringRunIDs, ri.String())
			}
			p.l.Debug("allocated jobs", "nodeID", allocNodeID, " run ids ", stringRunIDs)

			// propagate this message to whole cluster
			payload, err := proto.Marshal(&pb.CommandScheduleJob{
				PeerId: allocNodeID,
				RunIds: stringRunIDs,
			})
			if err != nil {
				p.errChan <- err
				return
			}
			if err := p.clusterManager.ApplyCommand(&pb.CommandLog{
				Type:    pb.CommandLogType_COMMAND_LOG_TYPE_SCHEDULE_JOB,
				Payload: payload,
			}); err != nil {
				p.errChan <- err
				return
			}

			// once the command is committed to raft log, we need to update the job state
			// from pending to accepted
			for _, runID := range allocRunIDs {
				if err := p.jobRunRepoFac.New().UpdateStatus(ctx, runID, models.RunStateAccepted); err != nil {
					p.errChan <- err
					return
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

// getJobAllocations looks for job runs which are in pending state that means
// they are not allocated to any peer for execution. This allocation is mostly
// done based on which node has most capacity available for execution.
// We assume if in case a node goes down, it will come back up for sure
// and we will not scale down the cluster once its scaled up. This is a
// temporary approach and ideally we should timeout jobs which are assigned
// to nodes which went down and move them back to the pending state list.
func (p *Planner) getJobAllocations(ctx context.Context) (mostCapNodeID string, runIDs []uuid.UUID, err error) {
	pendingJobRuns, err := p.jobRunRepoFac.New().GetByTrigger(ctx, models.TriggerManual, models.RunStatePending)
	if err != nil {
		return
	}

	// find which node has most capacity
	peerUtilization := map[string]int{}
	for _, mem := range p.clusterManager.GetClusterMembers() {
		// it is possible that the allocation state is empty so we need to initialize
		// the map with 0 for all cluster members first
		peerUtilization[mem.Name] = 0
	}
	currentState := p.clusterManager.GetState()
	for nodeID, allocationSet := range currentState.Allocation {
		for _, rawAlloc := range allocationSet.Values() {
			alloc := rawAlloc.(gossip.StateJob)
			if alloc.Status == models.RunStateAccepted.String() ||
				alloc.Status == models.RunStateRunning.String() {
				peerUtilization[nodeID]++
			}
		}
	}
	var mostCapSize = NodePoolSize
	for nodeID, utilization := range peerUtilization {
		if utilization < mostCapSize {
			mostCapNodeID = nodeID
			mostCapSize = utilization
		}
	}
	// cluster is full at the moment
	if mostCapNodeID == "" {
		return
	}

	for idx, pendingRun := range pendingJobRuns {
		runIDs = append(runIDs, pendingRun.ID)
		if idx+mostCapSize > NodePoolSize {
			break
		}
	}
	return
}

// leaderJobReconcile should update the job run state from running to
// terminating state like success/failed once all of the child instances
// are done executing and commit to cluster WAL
// Reconciliation happens by reading states from database as db is currently
// used to store shared state although raft WAL should be enough
//
// There are multiple ways we can allow non-leader nodes to notify about execution
// states, some are:
// - Using database as a shared store, non-leaders can directly update each
// instance status when the process is completed.
// - Use serf gossip protocol to broadcast job states to all the nodes
// until leader listens to it and leader send a ack message back as well as
// to raft wal.
// - Find raft leader by pinging each node and cache it, then create a RPC
// connection to directly send the message to leader, leader send a ack
// message back as well as to raft wal.
// I went with the second approach, that is using gossip to notify leader.
func (p *Planner) leaderJobReconcile(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	loopIdx := 0
	runRepo := p.jobRunRepoFac.New()
	instanceRepo := p.instanceRepoFac.New()
	for {
		if !p.clusterManager.IsLeader() {
			time.Sleep(NonLeaderSleepTime)
			continue
		}

		// check for non assignment, non terminating states
		waitingJobs, err := runRepo.GetByTrigger(ctx, models.TriggerManual, models.RunStateAccepted, models.RunStateRunning)
		if err != nil {
			p.errChan <- err
			continue
		}
		for jobIdx, currentRun := range waitingJobs {
			// check if this run is assigned to a node
			// if the job is in non terminating, non assignment state and its not
			// assigned to a node, we must have lost our WAL, mark it to be rescheduled
			var allocatedNode string
			for nodeID, allocSet := range p.clusterManager.GetState().Allocation {
				if allocatedNode != "" {
					break
				}
				for _, rawAlloc := range allocSet.Values() {
					alloc := rawAlloc.(gossip.StateJob)
					if alloc.UUID == currentRun.ID.String() {
						allocatedNode = nodeID
						break
					}
				}
			}
			if allocatedNode == "" {
				// move it back to assignment
				if err := runRepo.Clear(ctx, currentRun.ID); err != nil {
					p.errChan <- err
				}
				p.l.Debug("cleared orphaned run for reassignment", "run id", currentRun.ID)
				continue
			}

			// check if we need to update the run state inferred from instance states
			finalState := currentRun.Status
			instanceStates := map[models.JobRunState]int{}
			for _, instance := range currentRun.Instances {
				instanceStates[instance.Status]++
			}

			if instanceStates[models.RunStateFailed] > 0 {
				finalState = models.RunStateFailed
			} else if instanceStates[models.RunStateSuccess] == len(currentRun.Instances) && len(currentRun.Instances) > 0 {
				finalState = models.RunStateSuccess
			} else if instanceStates[models.RunStateRunning] > 0 || len(currentRun.Instances) > 0 {
				// if anyone is in running or no instances created so far for this job run
				// we should be in running or no node has picked this run yet
				finalState = models.RunStateRunning
			}

			if finalState != currentRun.Status {
				waitingJobs[jobIdx].Status = finalState

				// propagate this message to whole cluster
				payload, err := proto.Marshal(&pb.CommandUpdateJob{
					Patches: []*pb.CommandUpdateJob_Patch{
						{
							RunId:  currentRun.ID.String(),
							Status: finalState.String(),
						},
					},
					PeerId: allocatedNode,
				})
				if err != nil {
					p.errChan <- err
					continue
				}
				if err := p.clusterManager.ApplyCommand(&pb.CommandLog{
					Type:    pb.CommandLogType_COMMAND_LOG_TYPE_UPDATE_JOB,
					Payload: payload,
				}); err != nil {
					p.errChan <- err
					continue
				}
				if err := runRepo.UpdateStatus(ctx, currentRun.ID, finalState); err != nil {
					p.errChan <- err
					continue
				}
			}
		}

		for _, jobRun := range waitingJobs {
			if err := p.gcZombieInstances(ctx, instanceRepo, jobRun); err != nil {
				p.errChan <- err
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(JobReconcileSleepTime):
			// repeats loop:
			loopIdx++
		}
	}
}

func (p *Planner) leaderClusterEventHandler(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	instanceRepo := p.instanceRepoFac.New()

	for {
		if !p.clusterManager.IsLeader() {
			time.Sleep(NonLeaderSleepTime)
			continue
		}

		select {
		case evt := <-p.clusterManager.BroadcastListener():
			switch GossipCmdType(evt.Name) {
			case GossipCmdTypeInstanceCreate:
				err := evt.Respond([]byte("ok"))
				if err != nil {
					p.errChan <- err
					break
				}

				var payload GossipInstanceCreateRequest
				if err := json.Unmarshal(evt.Payload, &payload); err != nil {
					p.errChan <- err
					break
				}
				p.l.Info("serf query request", "payload", payload)

				if err := p.jobRunRepoFac.New().AddInstance(ctx, payload.RunID, models.InstanceSpec{
					ID:     payload.InstanceID,
					Name:   payload.Name,
					Type:   payload.Type,
					Status: models.RunStateAccepted,
				}); err != nil {
					p.errChan <- err
					break
				}
			case GossipCmdTypeInstanceStatus:
				err := evt.Respond([]byte("ok"))
				if err != nil {
					p.errChan <- err
					break
				}

				var payload GossipInstanceUpdateRequest
				if err := json.Unmarshal(evt.Payload, &payload); err != nil {
					p.errChan <- err
					break
				}
				p.l.Info("serf query request", "payload", payload)

				if err := instanceRepo.UpdateStatus(ctx, payload.InstanceID, payload.RunState); err != nil {
					p.errChan <- err
					break
				}
			default:
				p.l.Warn("unknown broadcast event request", "type", evt.String())
			}
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			// default case: repeats loop
		}
	}
}
