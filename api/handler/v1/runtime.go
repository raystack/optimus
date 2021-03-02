package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "github.com/odpf/optimus/api/proto/v1"
	log "github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) (*pb.JobSpecification, error)

	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification

	FromInstanceProto(*pb.InstanceSpec) (models.InstanceSpec, error)
	ToInstanceProto(models.InstanceSpec) (*pb.InstanceSpec, error)
}

type RuntimeServiceServer struct {
	version            string
	jobSvc             models.JobService
	adapter            ProtoAdapter
	projectRepoFactory ProjectRepoFactory
	instSvc            models.InstanceService
	scheduler          models.SchedulerUnit

	progressObserver progress.Observer
	Now              func() time.Time

	pb.UnimplementedRuntimeServiceServer
}

func (sv *RuntimeServiceServer) Version(ctx context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) {
	log.I(fmt.Printf("client with version %s requested for ping ", version.Client))
	response := &pb.VersionResponse{
		Server: sv.version,
	}
	return response, nil
}

func (sv *RuntimeServiceServer) DeploySpecification(req *pb.DeploySpecificationRequest, respStream pb.RuntimeService_DeploySpecificationServer) error {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	var jobsToKeep []models.JobSpec
	for _, reqJob := range req.GetJobs() {
		adaptJob, err := sv.adapter.FromJobProto(reqJob)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("%s: cannot adapt job %s", err.Error(), reqJob.GetName()))
		}

		err = sv.jobSvc.Create(adaptJob, projSpec)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("%s: failed to save %s", err.Error(), adaptJob.Name))
		}
		jobsToKeep = append(jobsToKeep, adaptJob)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobSyncObserver{
		stream: respStream,
	})

	// delete specs not sent for deployment
	// currently we don't support deploying a single dag at a time so this will change
	// once we do that
	if err := sv.jobSvc.KeepOnly(projSpec, jobsToKeep, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("%s: failed to delete jobs", err.Error()))
	}

	if err := sv.jobSvc.Sync(projSpec, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("%s: failed to sync jobs", err.Error()))
	}

	return nil
}

func (sv *RuntimeServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	if err := projectRepo.Save(sv.adapter.FromProjectProto(req.GetProject())); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save project %s", err.Error(), req.GetProject().GetName()))
	}

	return &pb.RegisterProjectResponse{
		Success: true,
		Message: "saved successfully",
	}, nil
}

func (sv *RuntimeServiceServer) ListProjects(ctx context.Context,
	req *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projects, err := projectRepo.GetAll()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to retrive saved projects", err.Error()))
	}

	projSpecsProto := []*pb.ProjectSpecification{}
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, sv.adapter.ToProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		All: projSpecsProto,
	}, nil
}

func (sv *RuntimeServiceServer) RegisterInstance(ctx context.Context, req *pb.RegisterInstanceRequest) (*pb.RegisterInstanceResponse, error) {
	jobScheduledTime, err := ptypes.Timestamp(req.GetScheduledAt())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse schedule time of job %s", err.Error(), req.GetScheduledAt()))
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	jobSpec, err := sv.jobSvc.GetByName(req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: job %s not found", err.Error(), req.GetJobName()))
	}
	jobProto, err := sv.adapter.ToJobProto(jobSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: cannot adapt job %s", err.Error(), jobSpec.Name))
	}

	instance, err := sv.instSvc.Register(jobSpec, jobScheduledTime, models.InstanceType(req.Type))
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to register instance of job %s", err.Error(), req.GetJobName()))
	}
	instanceProto, err := sv.adapter.ToInstanceProto(instance)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: cannot adapt instance for job %s", err.Error(), jobSpec.Name))
	}

	return &pb.RegisterInstanceResponse{
		Project:  sv.adapter.ToProjectProto(projSpec),
		Job:      jobProto,
		Instance: instanceProto,
	}, nil
}

func (sv *RuntimeServiceServer) JobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.
	JobStatusResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	jobStatuses, err := sv.scheduler.GetJobStatus(ctx, projSpec, req.GetJobName())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to fetch jobStatus %s", err.Error(),
			req.GetJobName()))
	}

	adaptedJobStatus := []*pb.JobStatus{}
	for _, jobStatus := range jobStatuses {
		ts, err := ptypes.TimestampProto(jobStatus.ScheduledAt)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse time for %s", err.Error(),
				req.GetJobName()))
		}
		adaptedJobStatus = append(adaptedJobStatus, &pb.JobStatus{
			State:       jobStatus.State.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobStatusResponse{
		All: adaptedJobStatus,
	}, nil
}

func NewRuntimeServiceServer(version string, jobSvc models.JobService,
	projectRepoFactory ProjectRepoFactory, adapter ProtoAdapter,
	progressObserver progress.Observer, instSvc models.InstanceService,
	scheduler models.SchedulerUnit) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		version:            version,
		jobSvc:             jobSvc,
		adapter:            adapter,
		projectRepoFactory: projectRepoFactory,
		progressObserver:   progressObserver,
		instSvc:            instSvc,
		scheduler:          scheduler,
	}
}

type jobSyncObserver struct {
	stream pb.RuntimeService_DeploySpecificationServer
	log    logrus.FieldLogger
}

func (obs *jobSyncObserver) Notify(e progress.Event) {
	switch evt := e.(type) {
	case *job.EventJobUpload:
		resp := &pb.DeploySpecificationResponse{
			Success: true,
			JobName: evt.Job.Name,
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Message = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send deploy spec ack for: %s", evt.Job.Name))
		}
	}
}
