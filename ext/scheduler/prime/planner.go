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
)

const (
	// NodePoolSize is fixed at the moment, should be configurable
	// this dictates how many jobs can be executed by a single optimus node
	// at a time
	NodePoolSize = 20

	// NonLeaderSleepTime is used to mark routine sleep if its a follower
	NonLeaderSleepTime = time.Second * 3

	// JobReconcileSleepTime is used to decide frequency of updating job run states
	JobReconcileSleepTime = time.Second * 5

	// InstanceRunTimeout will mark an instance as failed if a instance is in
	// running state more than provided time
	InstanceRunTimeout = time.Hour * 6

	// NodeJobRunTimeout will clear job run move it back to pending state to be
	// handled by another/same peer again
	NodeJobRunTimeout = time.Hour * 1
)

// GossipCmdType are serf user defined query commands
type GossipCmdType string

const (
	GossipCmdTypeInstanceCreate GossipCmdType = "INSTANCE_CREATE"
	GossipCmdTypeInstanceStatus GossipCmdType = "INSTANCE_STATUS"
)

type GossipInstanceCreateRequest struct {
	RunID      uuid.UUID
	InstanceID uuid.UUID
	Name       string
	Type       models.InstanceType
}

type GossipInstanceUpdateRequest struct {
	InstanceID uuid.UUID
	RunState   models.JobRunState
}

type ClusterManager interface {
	IsLeader() bool
	ApplyCommand(*pb.CommandLog) error
	GetState() gossip.State
	GetClusterMembers() []serf.Member
	GetLocalMember() serf.Member

	// BroadcastQuery used by cluster nodes for custom events
	BroadcastQuery(string, []byte) ([]byte, error)
	// BroadcastListener used by leader to listen cluster node custom events
	BroadcastListener() <-chan *serf.Query
}

// Planner creates an execution plan by monitoring
// all the connected peers and assigning what to execute to each node
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

	// leader only
	go p.leaderJobAllocation(ctx)
	go p.leaderJobReconcile(ctx)
	go p.leaderClusterEventHandler(ctx)

	// leader and worker
	go p.nodeJobExecution(ctx)

	return nil
}

func (p *Planner) Close() error {
	p.wg.Wait()
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
