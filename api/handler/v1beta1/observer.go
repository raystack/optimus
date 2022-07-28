package v1beta1

import (
	"fmt"
	"sync"

	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/models"
)

type resourceObserver struct {
	stream pb.ResourceService_DeployResourceSpecificationServer
	log    log.Logger
	mu     *sync.Mutex
}

func NewResourceObserver(
	stream pb.ResourceService_DeployResourceSpecificationServer,
	log log.Logger,
	mu *sync.Mutex,
) progress.Observer {
	return &resourceObserver{
		stream: stream,
		log:    log,
		mu:     mu,
	}
}

func (obs *resourceObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	var (
		success               = true
		resourceName, message string
	)

	switch evt := e.(type) {
	case *datastore.EventResourceCreated:
		resourceName = evt.Spec.Name
		if evt.Err != nil {
			success = false
			message = evt.Err.Error()
		} else {
			message = evt.String()
		}
	case *datastore.EventResourceUpdated:
		resourceName = evt.Spec.Name
		if evt.Err != nil {
			success = false
			message = evt.Err.Error()
		} else {
			message = evt.String()
		}
	case *datastore.EventResourceSkipped:
		message = evt.String()
	default:
		return
	}

	resp := &pb.DeployResourceSpecificationResponse{
		Success:      success,
		Ack:          true,
		ResourceName: resourceName,
		Message:      message,
	}
	if err := obs.stream.Send(resp); err != nil {
		obs.log.Error("failed to send deploy spec ack", "spec name", resourceName, "error", err)
	}
}

type jobCheckObserver struct {
	stream pb.JobSpecificationService_CheckJobSpecificationsServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *jobCheckObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	switch evt := e.(type) {
	case *models.ProgressJobCheckFailed:
		resp := &pb.CheckJobSpecificationsResponse{
			Success: false,
			Ack:     true,
			JobName: evt.Name,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send check ack", "job name", evt.Name, "error", err)
		}
	case *models.ProgressJobCheckSuccess:
		resp := &pb.CheckJobSpecificationsResponse{
			Success: true,
			Ack:     true,
			JobName: evt.Name,
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send check ack", "job name", evt.Name, "error", err)
		}
	}
}

type jobDeploymentObserver struct {
	stream pb.JobSpecificationService_DeployJobSpecificationServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *jobDeploymentObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	resp := &pb.DeployJobSpecificationResponse{}
	switch evt := e.(type) {
	case *models.ProgressJobUpload:
		resp.Success = true
		resp.Type = evt.Type()
		resp.JobName = evt.Name

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	case *models.JobDeleteEvent:
		resp.Success = true
		resp.JobName = evt.Name
		resp.Value = evt.String()
		resp.Type = evt.Type()

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	case *models.JobCreateEvent:
		resp.Success = true
		resp.JobName = evt.Name
		resp.Value = evt.String()
		resp.Type = evt.Type()

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	case *models.JobModifyEvent:
		resp.Success = true
		resp.JobName = evt.Name
		resp.Value = evt.String()
		resp.Type = evt.Type()

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	case *models.ProgressJobDependencyResolution:
		resp.Success = true
		resp.JobName = evt.Job
		resp.Value = evt.String()
		resp.Type = evt.Type()

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	case *models.ProgressJobSpecUnknownDependencyUsed:
		resp.Success = true
		resp.JobName = evt.Job
		resp.Value = evt.String()
		resp.Type = evt.Type()

	case *models.ProgressJobDeploymentRequestCreated:
		resp.Success = true
		resp.Value = evt.ID().UUID().String()
		resp.Type = evt.Type()

		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
	default:
		obs.log.Warn(fmt.Sprintf("unknown event type: %+v", e))
		return
	}

	if err := obs.stream.Send(resp); err != nil {
		obs.log.Error("failed to send", resp.GetType(), "evt", e.String(), "error", err)
	}
}
