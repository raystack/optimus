package prime

import (
	"context"
	"sync"
	"time"

	"github.com/odpf/salt/log"

	"github.com/google/uuid"
	"github.com/hashicorp/serf/serf"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/cluster/v1beta1"
	"github.com/odpf/optimus/core/gossip"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"google.golang.org/protobuf/proto"
)

const (
	// PeerPoolSize is fixed at the moment, should be configurable
	// this dictates how many jobs can be executed by a single optimus peer
	// at a time
	PeerPoolSize = 20

	SleepTime = time.Second * 10

	// InstanceRunTimeout will mark an instance as failed if a instance is in
	// running state more than provided time
	InstanceRunTimeout = time.Hour * 4

	// NodeJobRunTimeout will clear job run move it back to pending state to be
	// handled by another/same peer again
	NodeJobRunTimeout = time.Hour * 1
)

type ClusterManager interface {
	IsLeader() bool
	ApplyCommand(*pb.CommandLog) error
	GetState() gossip.State
	GetClusterMembers() []serf.Member
	GetLocalMember() serf.Member
}

// Planners creates an execution plan by monitoring
// all the connected peers and assigning
type Planner struct {
	l log.Logger

	clusterManager  ClusterManager
	jobRunRepoFac   RunRepoFactory
	instanceRepoFac InstanceRepoFactory
	executor        models.ExecutorUnit
	uuidProvider    utils.UUIDProvider

	wg      *sync.WaitGroup
	errChan chan error
	now     func() time.Time
}

func (p *Planner) Init(ctx context.Context) error {
	go func() {
		for err := range p.errChan {
			p.l.Error("planner error accumulator", "error", err)
		}
	}()
	go p.leaderJobAllocation(ctx)
	go p.leaderJobReconcile(ctx)
	go p.peerJobExecution(ctx)
	return nil
}

func (p *Planner) Close() error {
	p.wg.Wait()
	return nil
}

func (p *Planner) leaderJobAllocation(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	loopIdx := 0
	for {
		if !p.clusterManager.IsLeader() {
			time.Sleep(SleepTime)
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
		default:
			loopIdx++
			time.Sleep(SleepTime)
		}
	}
}

// getJobAllocations looks for job runs which are in pending state that means
// they are not allocated to any peer for execution. This allocation is mostly
// done based on which node has most capacity available for execution.
// We assume if in case a node goes down, it will come back up for sure
// and we will not scale down the cluster once its scaled up. This is a
// temporary approach and ideally we should timeout jobs which are assigned
// to jobs which went down and move them back to the pending state list.
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
	mostCapSize := PeerPoolSize
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
		if idx+mostCapSize > PeerPoolSize {
			break
		}
	}
	return
}

// leaderJobReconcile should update the job run state from running to
// terminating state like success/failed once all of the child instances
// are done executing and commit to cluster WAL
func (p *Planner) leaderJobReconcile(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()
	loopIdx := 0
	for {
		if !p.clusterManager.IsLeader() {
			time.Sleep(SleepTime)
			continue
		}

		runRepo := p.jobRunRepoFac.New()
		// check for non assignment, non terminating states
		waitingJobs, err := runRepo.GetByTrigger(ctx, models.TriggerManual, models.RunStateAccepted, models.RunStateRunning)
		if err != nil {
			p.errChan <- err
			continue
		}
		for _, currentRun := range waitingJobs {
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

		select {
		case <-ctx.Done():
			return
		default:
			loopIdx++
			time.Sleep(SleepTime)
		}
	}
}

// peerJobExecution looks for job assigned to this node and executes them
func (p *Planner) peerJobExecution(ctx context.Context) {
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
				if alloc.Status == models.ReplayStatusAccepted {
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
		default:
			loopIdx++
			time.Sleep(SleepTime)
		}
	}
}

// executeRun finds all tasks/hooks that belong to this run job spec and
// execute them in order
// As each context gets executed, its state should be updated in job run
// instance store
func (p *Planner) executeRun(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun) error {
	// for each exec instance in job
	// - check if job run instances(tasks/hooks) are already in finished state
	// - if not find which one is not and create/execute them
	// for now we will only support executing each task of a job and ignore
	// all attached hooks

	instanceRepo := p.instanceRepoFac.New()

	// first check if this task is already in terminating state
	for _, instance := range jobRun.Instances {
		if instance.Type == models.InstanceTypeTask &&
			instance.Name == jobRun.Spec.Task.Unit.Info().Name {
			if instance.Status == models.RunStateSuccess || instance.Status == models.RunStateFailed {
				// already finished
				return nil
			}

			if instance.Status == models.RunStateRunning &&
				instance.UpdatedAt.Add(InstanceRunTimeout).After(p.now()) {
				// heartbeat timeout, mark task zombie
				p.l.Warn("found a zombie instance", "job name", jobRun.Spec.Name, "task name", jobRun.Spec.Task.Unit.Info().Name)

				// cancel task and move back state to accepted
				if err := instanceRepo.UpdateStatus(ctx, instance.ID, models.RunStateAccepted); err != nil {
					return err
				}

				return p.executor.Stop(ctx, models.ExecutorStopRequest{
					ID:     instance.ID.String(),
					Signal: "SIGKILL",
				})
			}
		}
	}

	// create an instance
	instanceID, err := p.uuidProvider.NewUUID()
	if err != nil {
		return err
	}
	newInstance := models.InstanceSpec{
		ID:     instanceID,
		Name:   jobRun.Spec.Task.Unit.Info().Name,
		Type:   models.InstanceTypeTask,
		Status: models.RunStateAccepted,
	}
	if err := p.jobRunRepoFac.New().AddInstance(ctx, namespace, jobRun, newInstance); err != nil {
		return err
	}

	// send it to executor for execution
	p.l.Info("starting executing job", "job name", jobRun.Spec.Name)
	_, err = p.executor.Start(ctx, models.ExecutorStartRequest{
		ID:        newInstance.ID.String(),
		Job:       jobRun.Spec,
		Namespace: namespace,
	})
	if err != nil {
		return err
	}
	if err := p.instanceRepoFac.New().UpdateStatus(ctx, instanceID, models.RunStateRunning); err != nil {
		return err
	}

	// block until the given task finishes
	finishChan, err := p.executor.WaitForFinish(ctx, newInstance.ID.String())
	if err != nil {
		return err
	}
	finishCode := <-finishChan
	if finishCode != 0 {
		p.l.Warn("job finished with non zero code", "code", finishCode, "job name", jobRun.Spec.Name)

		// mark instance failed
		if err := instanceRepo.UpdateStatus(ctx, newInstance.ID, models.RunStateFailed); err != nil {
			return err
		}
	}

	// mark instance success
	if err := instanceRepo.UpdateStatus(ctx, newInstance.ID, models.RunStateSuccess); err != nil {
		return err
	}
	p.l.Info("finished executing job spec", "job name", jobRun.Spec.Name)
	return nil
}

func NewPlanner(l log.Logger, sv ClusterManager, jobRunRepoFac RunRepoFactory,
	instanceRepoFactory InstanceRepoFactory, uuidProvider utils.UUIDProvider,
	executor models.ExecutorUnit, now func() time.Time) *Planner {
	return &Planner{
		l:               l,
		clusterManager:  sv,
		jobRunRepoFac:   jobRunRepoFac,
		instanceRepoFac: instanceRepoFactory,
		executor:        executor,
		uuidProvider:    uuidProvider,
		now:             now,
		wg:              new(sync.WaitGroup),
		errChan:         make(chan error),
	}
}
