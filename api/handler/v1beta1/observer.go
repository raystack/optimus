package v1beta1

import (
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

func (obs *resourceObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	evt, ok := e.(*datastore.EventResourceUpdated)
	if ok {
		resp := &pb.DeployResourceSpecificationResponse{
			Success:      true,
			Ack:          true,
			ResourceName: evt.Spec.Name,
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Message = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send deploy spec ack", "spec name", evt.Spec.Name, "error", err)
		}
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

type jobRefreshObserver struct {
	stream pb.JobSpecificationService_RefreshJobsServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *jobRefreshObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	switch evt := e.(type) {
	case *models.ProgressJobUpload:
		resp := &pb.RefreshJobsResponse{
			Success: true,
			JobName: evt.Name,
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send refresh ack", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobSpecUnknownDependencyUsed:
		resp := &pb.RefreshJobsResponse{
			JobName: evt.Job,
			Value:   evt.String(),
			Success: false,
			Type:    evt.Type(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send unknown dependency notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobDependencyResolution:
		resp := &pb.RefreshJobsResponse{
			JobName: evt.Job,
			Value:   evt.String(),
			Success: true,
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send failed dependency resolution notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobDeploymentRequestCreated:
		resp := &pb.RefreshJobsResponse{
			Value:   evt.ID().UUID().String(),
			Success: true,
			Type:    evt.Type(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send job deployment request created", "evt", evt.String(), "error", err)
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

	switch evt := e.(type) {
	case *models.ProgressJobUpload:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			Type:    evt.Type(),
			JobName: evt.Name,
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send deploy spec ack", "evt", evt.String(), "error", err)
		}
	case *models.ProgressSavedJobDelete:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			JobName: evt.Name,
			Value:   evt.String(),
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send delete notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressSavedJobCreate:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			JobName: evt.Name,
			Value:   evt.String(),
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send create notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressSavedJobModify:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			JobName: evt.Name,
			Value:   evt.String(),
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send modify notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobDependencyResolution:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			JobName: evt.Job,
			Value:   evt.String(),
			Type:    evt.Type(),
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Value = evt.Err.Error()
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send dependency resolution notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobSpecUnknownDependencyUsed:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			JobName: evt.Job,
			Value:   evt.String(),
			Type:    evt.Type(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send unknown dependency notification", "evt", evt.String(), "error", err)
		}
	case *models.ProgressJobDeploymentRequestCreated:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			Value:   evt.ID().UUID().String(),
			Type:    evt.Type(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send job deployment request created", "evt", evt.String(), "error", err)
		}
	}
}
