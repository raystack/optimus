package gossip

import (
	"context"
	"expvar"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/odpf/salt/log"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hashicorp/serf/serf"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/cluster/v1beta1"
	"github.com/odpf/optimus/config"
	"google.golang.org/protobuf/proto"
)

const (
	raftLogDBPath       = "wal.db"
	retainSnapshotCount = 3
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second

	applyTimeout = 10 * time.Second
	devMode      = true
	//leaderWaitDelay  = 100 * time.Millisecond
	//appliedWaitDelay = 100 * time.Millisecond
	//raftLogCacheSize = 512

)

var (
	debugIsRaftLeader = expvar.NewString("IsRaftLeader")
)

type Server struct {
	l   log.Logger
	fsm *fsm

	raft             *raft.Raft
	serf             *serf.Serf
	serfEvents       chan serf.Event
	serfQueryEventListener chan *serf.Query
	raftBootstrapped bool
}

// isFirstNode check if the currently running node is indexed at 1
// we mark the first node as the one that should bootstrap raft cluster if it is
// not already bootstrapped
func isFirstNode(l log.Logger, id string) bool {
	nodeIDSplits := strings.Split(id, "-")
	lastStr := nodeIDSplits[len(nodeIDSplits)-1]
	idx, err := strconv.Atoi(lastStr)
	if err != nil {
		l.Fatal("node name should end with hyphen integer, like node-1")
	}
	if lastStr != "" && idx == 1 {
		return true
	}
	return false
}

// Init initializes the node
// Node first starts up a raft instance, it prepares a WAL and starts
// listening on a port for raft log events, if any new event comes, it
// gets deserialized and appended to its local WAL.
// Internally this WAL is only committed if majority of the cluster members
// have acked that they have received the event.
// Raft events are only sent by raft leader.
// Raft cluster runs with fixed number of nodes, and to change this membership
// cluster will use a gossip protocol. Gossip will let this fixed raft
// cluster know when to modify the membership of existing or new members.
// Cluster will start with just one node for start, when another new node
// is started and have a total of 2 nodes, gossip protocol will add
// this new node to existing raft cluster.
// Starting more nodes, again the gossip protocol  will modify
// the cluster and increase the cluster size as needed.
// Default when the cluster is started, first node will elect itself
// as leader.
// Once the consensus is reached, removing a member from cluster
// membership won't start leader re-election(unless its a leader)
// and won't modify the raft membership. Cluster will believe that
// a member is down temporarily and wait for it to be revived.
// To explicitly reduce the cluster size, that is removing node form raft
// membership, we need to let it know using gossip protocol gracefully.
// Recommended cluster size is 3 nodes which gives one node failover.
func (s *Server) Init(ctx context.Context, schedulerConf config.SchedulerConfig) error {
	if err := s.initRaft(ctx, devMode, isFirstNode(s.l, schedulerConf.NodeID), schedulerConf, s.fsm); err != nil {
		return err
	}
	s.l.Info(fmt.Sprintf("%v", s.raft.Stats()))

	if err := s.initSerf(ctx, schedulerConf); err != nil {
		return err
	}
	s.l.Info(fmt.Sprintf("%v", s.serf.Stats()))

	go s.handleSerfEvents()
	go s.syncLeader()

	return nil
}

func (s *Server) Shutdown() error {
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil {
			return err
		}
	}

	if s.serf != nil {
		// TODO(kushsharma): currently shutting down cluster
		// actually gracefully removes the current node from raft cluster.
		// We don't know if this leave was intentional so ideally we should
		// only leave a raft cluster when this was explicitly stated
		// maybe by using a CLI command sent to leader.
		// For now we will do this to keep development easy.
		if err := s.serf.Leave(); err != nil {
			return err
		}
		if err := s.serf.Shutdown(); err != nil {
			return err
		}
	}
	return nil
}

// ApplyCommand writes a new log entry in write ahead log of raft cluster
// this log is replicated to all the connected nodes
func (s *Server) ApplyCommand(cmd *pb.CommandLog) error {
	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	future := s.raft.ApplyLog(raft.Log{
		Type: raft.LogCommand,
		Data: cmdBytes,
	}, applyTimeout)
	return future.Error()
}

// GetClusterMembers return cluster active members
func (s *Server) GetClusterMembers() []serf.Member {
	return s.serf.Members()
}

// GetState returns current local wal state
func (s *Server) GetState() State {
	return s.fsm.state
}

// GetLocalMember returns self details from the cluster
func (s *Server) GetLocalMember() serf.Member {
	return s.serf.LocalMember()
}

// IsLeader checks if current node is the cluster leader at the moment
func (s *Server) IsLeader() bool {
	if s.raft == nil {
		return false
	}
	return s.raft.State() == raft.Leader
}

// BroadcastQuery broadcast a message to cluster expecting a reply from leader
func (s *Server) BroadcastQuery(cmd string, payload []byte) ([]byte, error) {
	qResp, err := s.serf.Query(cmd, payload, s.serf.DefaultQueryParams())
	if err != nil {
		return nil, err
	}
	var response []byte
	for !qResp.Finished() {
		select {
		case resp := <-qResp.ResponseCh():
			s.l.Info("serf query response from", "node", resp.From)
			response = resp.Payload
			// we don't wait for others to respond because only leader should respond back
			qResp.Close()
		}
	}
	if response == nil {
		return nil, fmt.Errorf("time out")
	}
	return response, nil
}

func (s *Server) BroadcastListener() <- chan *serf.Query {
	return s.serfQueryEventListener
}

// serf is used for managing membership of peers across cluster
// if a node joins it gets added to cluster to take part as voter
// if a node leaves membership gossip, it is removed from the raft cluster
func (s *Server) initSerf(ctx context.Context, schedulerConf config.SchedulerConfig) error {
	s.serfEvents = make(chan serf.Event)
	s.serfQueryEventListener = make(chan *serf.Query)
	serfConfig, err := newSerfConfig(schedulerConf.GossipAddr, schedulerConf.RaftAddr, schedulerConf.NodeID, s.serfEvents)
	if err != nil {
		return err
	}

	if s.serf, err = serf.Create(serfConfig); err != nil {
		return err
	}

	peers := strings.Split(schedulerConf.Peers, ",")
	if len(peers) > 0 && peers[0] != "" {
		if _, err := s.serf.Join(peers, true); err != nil {
			return err
		}
	}
	return nil
}

func newSerfConfig(serfAddr, raftAddress, nodeID string, eventCh chan serf.Event) (*serf.Config, error) {
	serfHost, serfPort, err := net.SplitHostPort(serfAddr)
	if err != nil {
		return nil, err
	}
	serfPortInt, err := strconv.Atoi(serfPort)
	if err != nil {
		return nil, err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = serfHost
	config.MemberlistConfig.BindPort = serfPortInt
	config.NodeName = nodeID
	config.Tags = map[string]string{}
	config.Tags["raftAddr"] = raftAddress
	config.Tags["nodeID"] = nodeID
	config.EventCh = eventCh
	config.EnableNameConflictResolution = false
	return config, nil
}

// raft manages the leadership/follower state in cluster
// minimum 3 nodes are required to work properly to have 1 node
// fail-over resistant
func (s *Server) initRaft(ctx context.Context, devMode bool, bootstrapCluster bool, schedulerConf config.SchedulerConfig, fsm raft.FSM) error {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(schedulerConf.NodeID)

	logStore, stableStore, snapshotStore, err := s.initRaftStore(devMode, filepath.Join(schedulerConf.DataDir, schedulerConf.NodeID))
	if err != nil {
		return err
	}

	addr, err := net.ResolveTCPAddr("tcp", schedulerConf.RaftAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(schedulerConf.RaftAddr, addr, connectionPoolCount, connectionTimeout, os.Stderr)
	if err != nil {
		return err
	}

	s.raft, err = raft.NewRaft(c, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("raft.InitRaft: %v", err)
	}

	if s.raftBootstrapped, err = raft.HasExistingState(logStore, stableStore, snapshotStore); err != nil {
		return err
	}
	if !s.raftBootstrapped && bootstrapCluster {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(schedulerConf.NodeID),
					Address:  transport.LocalAddr(),
				},
			},
		}
		f := s.raft.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return fmt.Errorf("raft.raft.BootstrapCluster: %v", err)
		}
	}
	return nil
}

func (s *Server) initRaftStore(devMode bool, baseDir string) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if devMode {
		inMemStore := raft.NewInmemStore()
		discardStore := raft.NewDiscardSnapshotStore()
		return inMemStore, inMemStore, discardStore, nil
	}

	// prepare directory for data
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, nil, nil, err
	}

	// use embedded boltdb
	boltDB, err := boltdb.NewBoltStore(filepath.Join(baseDir, raftLogDBPath))
	if err != nil {
		return nil, nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, raftLogDBPath), err)
	}

	// use embedded filesystem
	fss, err := raft.NewFileSnapshotStore(baseDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}
	return boltDB, boltDB, fss, nil
}

// handleSerfEvents listens for gossip across cluster
// normally only leader will take actions during any membership activity
func (s *Server) handleSerfEvents() {
	for evt := range s.serfEvents {
		isLeader := s.raft.State() == raft.Leader
		s.l.Info(fmt.Sprintf("%v, leader: %v", evt, isLeader))
		if !isLeader {
			continue
		}
		var err error
		switch evt.EventType() {
		case serf.EventQuery:
			q := evt.(*serf.Query)
			s.serfQueryEventListener <- q
		case serf.EventMemberJoin:
			memEvt := evt.(serf.MemberEvent)
			for _, member := range memEvt.Members {
				err = s.HandleAddPeer(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			memEvt := evt.(serf.MemberEvent)
			for _, member := range memEvt.Members {
				err = s.HandleLeavePeer(member)
			}
		case serf.EventUser:
			s.l.Info("received a user event in gossip", "evt", evt)
		}

		if err != nil {
			s.l.Error("handleSerfEvents", "error", err)
		}
	}
}

// syncLeader listens for changes in leadership, if we gain or we lose
func (s *Server) syncLeader() {
	debugIsRaftLeader.Set("false")
	for isLeader := range s.raft.LeaderCh() {
		if !isLeader {
			debugIsRaftLeader.Set("false")
			continue
		}

		debugIsRaftLeader.Set("true")
		// although we are listening over serf events
		// doing it here as well makes sure we are in sync
		for _, member := range s.serf.Members() {
			var err error
			switch member.Status {
			case serf.StatusAlive:
				err = s.HandleAddPeer(member)
			case serf.StatusLeft, serf.StatusFailed:
				err = s.HandleLeavePeer(member)
			}
			if err != nil {
				s.l.Error("leader failed to sync member", "error", err.Error(),
					"member name", member.Name, "member status", member.Status)
			}
		}
	}
}

// HandleAddPeer adds a node to cluster
// only cluster leader should call this
func (s *Server) HandleAddPeer(member serf.Member) error {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.l.Info(fmt.Sprintf("failed to get raft configuration: %v", err))
		return err
	}
	s.l.Info(fmt.Sprintf("%v", configFuture.Configuration()))

	memID := raft.ServerID(member.Tags["nodeID"])
	memAddr := raft.ServerAddress(member.Tags["raftAddr"])
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == memID && server.Address == memAddr {
			// no need to add this peer, already added
			return nil
		}
	}

	if err := s.raft.AddVoter(memID, memAddr, 0, 0).Error(); err != nil {
		return err
	}
	s.l.Info("node added to cluster successfully ", member.Name)
	return nil
}

// HandleLeavePeer removes a node to cluster
// only cluster leader should call this
func (s *Server) HandleLeavePeer(member serf.Member) error {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.l.Info(fmt.Sprintf("failed to get raft configuration: %v", err))
		return err
	}
	s.l.Info(fmt.Sprintf("%v", configFuture.Configuration()))

	memID := raft.ServerID(member.Tags["nodeID"])
	inCluster := false
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == memID {
			inCluster = true
		}
	}
	if !inCluster {
		// no need to remove
		return nil
	}

	if err := s.raft.RemoveServer(memID, 0, 0).Error(); err != nil {
		return err
	}
	s.l.Info("node removed from cluster successfully", "member name", member.Name)
	return nil
}

func NewServer(l log.Logger, nowFn func() time.Time) *Server {
	return &Server{
		l:   l,
		fsm: NewFSM(l, nowFn),
	}
}
