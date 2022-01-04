package gossip

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/cluster/v1beta1"
	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/proto"
)

type State struct {
	Allocation map[string]set.Set // peerID -> StateJob
}

type StateJob struct {
	UUID   string
	Status string
}

type fsm struct {
	l log.Logger

	state State
	mu    *sync.Mutex
}

// Apply applies a raft log entry to the local fsm store
func (f *fsm) Apply(rlog *raft.Log) interface{} {
	if rlog.Type != raft.LogCommand {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	cmd := &pb.CommandLog{}
	if err := proto.Unmarshal(rlog.Data, cmd); err != nil {
		return nil
	}

	f.l.Debug("apply wal", "command", cmd.Type.String())
	switch cmd.Type {
	case pb.CommandLogType_COMMAND_LOG_TYPE_SCHEDULE_JOB:
		cmdLog := &pb.CommandScheduleJob{}
		if err := proto.Unmarshal(cmd.Payload, cmdLog); err != nil {
			return nil
		}
		// update local state
		for _, id := range cmdLog.RunIds {
			if _, ok := f.state.Allocation[cmdLog.PeerId]; !ok {
				f.state.Allocation[cmdLog.PeerId] = set.NewHashSet()
			}

			f.state.Allocation[cmdLog.PeerId].Add(StateJob{
				UUID:   id,
				Status: models.RunStateAccepted.String(),
			})
		}
		f.l.Debug("updated state", "alloc state", f.state.Allocation[cmdLog.PeerId])
	case pb.CommandLogType_COMMAND_LOG_TYPE_UPDATE_JOB:
		cmdLog := &pb.CommandUpdateJob{}
		if err := proto.Unmarshal(cmd.Payload, cmdLog); err != nil {
			return nil
		}
		// update local state
		for _, patch := range cmdLog.Patches {
			for _, rawJobState := range f.state.Allocation[cmdLog.PeerId].Values() {
				jobState := rawJobState.(StateJob)
				if jobState.UUID == patch.RunId {
					f.state.Allocation[cmdLog.PeerId].Remove(jobState)
					jobState.Status = patch.Status

					// if run at terminating states, deallocate else update
					if patch.Status != models.RunStateFailed.String() &&
						patch.Status != models.RunStateSuccess.String() {
						f.state.Allocation[cmdLog.PeerId].Add(jobState)
					}
				}
			}
		}
		f.l.Debug("updated state", "alloc state", f.state.Allocation[cmdLog.PeerId])
	default:
		// ignore
	}
	return nil
}

// fsmSnapshot returns a snapshot of the local fsm store
// Note(kushsharma): we use golang gob for encoding the struct to bytes, this
// is slow compare to protobuf or msgpack but I think its good enough for
// now, can be optimised further if needed
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var b bytes.Buffer
	encoder := gob.NewEncoder(bufio.NewWriter(&b))
	if err := encoder.Encode(f.state); err != nil {
		return nil, err
	}
	return &fsmSnapshot{
		data: b.Bytes(),
	}, nil
}

// Restore stores the fsm store to the original state when
// starting/restarting
func (f *fsm) Restore(closer io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	defer closer.Close()
	decoder := gob.NewDecoder(closer)
	var state State
	if err := decoder.Decode(&state); err != nil {
		return err
	}
	f.state = state
	return nil
}

func NewFSM(l log.Logger) *fsm {
	return &fsm{
		l: l,
		state: State{
			Allocation: map[string]set.Set{},
		},
		mu: new(sync.Mutex),
	}
}

type fsmSnapshot struct {
	data []byte
}

// Persist save the snapshot to disk
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// write data
		if _, err := sink.Write(s.data); err != nil {
			return err
		}
		// close the sink
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return nil
}

func (s *fsmSnapshot) Release() {
	s.data = nil
}
