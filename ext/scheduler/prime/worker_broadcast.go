package prime

import (
	"encoding/json"
	"time"

	"github.com/hashicorp/go-multierror"
)

func (p *Planner) broadcastInstanceCreate(msg GossipInstanceCreateRequest) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	var errs []error
	for numOfRetries := 5; numOfRetries > 0; numOfRetries-- {
		response, err := p.clusterManager.BroadcastQuery(string(GossipCmdTypeInstanceCreate), payload)
		if err != nil {
			time.Sleep(time.Millisecond * 100)
			errs = append(errs, err)
			continue
		}
		p.l.Info("serf query response", "payload", string(response))
	}
	if len(errs) > 0 {
		// TODO(kushsharma): test this line
		return multierror.Append(nil, errs...)
	}
	return nil
}

func (p *Planner) broadcastInstanceStatus(msg GossipInstanceUpdateRequest) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	var errs []error
	for numOfRetries := 5; numOfRetries > 0; numOfRetries-- {
		response, err := p.clusterManager.BroadcastQuery(string(GossipCmdTypeInstanceStatus), payload)
		if err != nil {
			time.Sleep(time.Millisecond * 100)
			errs = append(errs, err)
			continue
		}
		p.l.Info("serf query response", "payload", string(response))
	}
	if len(errs) > 0 {
		// TODO(kushsharma): test this line
		return multierror.Append(nil, errs...)
	}
	return nil
}
