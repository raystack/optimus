package v1beta1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

var runtimeDeployJobSpecificationCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "runtime_deploy_jobspec",
	Help: "Number of jobs requested for deployment by runtime",
})

type JobSpecServiceServer struct {
	l                log.Logger
	jobSvc           models.JobService
	pluginRepo       models.PluginRepository
	projectService   service.ProjectService
	namespaceService service.NamespaceService
	progressObserver progress.Observer
	pb.UnimplementedJobSpecificationServiceServer
}

func (sv *JobSpecServiceServer) DeployJobSpecification(stream pb.JobSpecificationService_DeployJobSpecificationServer) error {
	startTime := time.Now()
	errNamespaces := []string{}

	for {
		observers := new(progress.ObserverChain)
		observers.Join(sv.progressObserver)
		observers.Join(&jobSyncObserver{
			stream: stream,
			log:    sv.l,
			mu:     new(sync.Mutex),
		})
		ctx := stream.Context()

		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			stream.Send(&pb.DeployJobSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: err.Error(),
			})
			return err // immediate error returned (grpc error level)
		}

		namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
		if err != nil {
			stream.Send(&pb.DeployJobSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: err.Error(),
			})
			errNamespaces = append(errNamespaces, req.NamespaceName)
			continue
		}

		jobSpecs := sv.convertProtoToJobSpec(req.GetJobs())

		// Deploying only the modified jobs
		if err := sv.jobSvc.Deploy(ctx, namespaceSpec, jobSpecs, observers); err != nil {
			stream.Send(&pb.DeployJobSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: fmt.Sprintf("failed to deploy jobs: \n%s", err.Error()),
			})
			errNamespaces = append(errNamespaces, req.NamespaceName)
			continue
		}

		// TODO: will be deleted
		if err := sv.jobSvc.Sync(stream.Context(), namespaceSpec, observers); err != nil {
			stream.Send(&pb.DeployJobSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: fmt.Sprintf("failed to sync jobs: \n%s", err.Error()),
			})
			errNamespaces = append(errNamespaces, req.NamespaceName)
			continue
		}
		runtimeDeployJobSpecificationCounter.Add(float64(len(req.Jobs)))
		stream.Send(&pb.DeployJobSpecificationResponse{
			Success: true,
			Ack:     true,
			Message: fmt.Sprintf("jobs with namespace [%s] are deployed successfully", req.NamespaceName),
		})
	}
	sv.l.Info("finished job deployment", "time", time.Since(startTime))
	if len(errNamespaces) > 0 {
		sv.l.Warn(fmt.Sprintf("there's error while deploying namespaces: [%s]", strings.Join(errNamespaces, ", ")))
		return fmt.Errorf("error when deploying: [%s]", strings.Join(errNamespaces, ", "))
	}
	return nil
}

func (sv *JobSpecServiceServer) convertProtoToJobSpec(jobs []*pb.JobSpecification) []models.JobSpec {
	if len(jobs) == 0 {
		return []models.JobSpec{}
	}

	var jobsToKeep []models.JobSpec
	for _, reqJob := range jobs {
		adaptJob, err := FromJobProto(reqJob, sv.pluginRepo)
		if err != nil {
			sv.l.Error(fmt.Sprintf("%s: cannot adapt job %s", err.Error(), reqJob.GetName()))
			continue
		}
		jobsToKeep = append(jobsToKeep, adaptJob)
	}

	return jobsToKeep
}

func (sv *JobSpecServiceServer) ListJobSpecification(ctx context.Context, req *pb.ListJobSpecificationRequest) (*pb.ListJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpecs, err := sv.jobSvc.GetAll(ctx, namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to retrieve jobs for project %s", err.Error(), req.GetProjectName())
	}

	jobProtos := []*pb.JobSpecification{}
	for _, jobSpec := range jobSpecs {
		jobProto := ToJobProto(jobSpec)

		jobProtos = append(jobProtos, jobProto)
	}
	return &pb.ListJobSpecificationResponse{
		Jobs: jobProtos,
	}, nil
}

func (sv *JobSpecServiceServer) CheckJobSpecification(ctx context.Context, req *pb.CheckJobSpecificationRequest) (*pb.CheckJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	j, err := FromJobProto(req.GetJob(), sv.pluginRepo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to adapt job %s", err.Error(), req.GetJob().Name)
	}
	reqJobs := []models.JobSpec{j}

	if err = sv.jobSvc.Check(ctx, namespaceSpec, reqJobs, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to compile jobs\n%s", err.Error())
	}
	return &pb.CheckJobSpecificationResponse{Success: true}, nil
}

func (sv *JobSpecServiceServer) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, respStream pb.JobSpecificationService_CheckJobSpecificationsServer) error {
	namespaceSpec, err := sv.namespaceService.Get(respStream.Context(), req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobCheckObserver{
		stream: respStream,
		log:    sv.l,
		mu:     new(sync.Mutex),
	})

	var reqJobs []models.JobSpec
	for _, jobProto := range req.GetJobs() {
		j, err := FromJobProto(jobProto, sv.pluginRepo)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: failed to adapt job %s", err.Error(), jobProto.Name)
		}
		reqJobs = append(reqJobs, j)
	}

	if err = sv.jobSvc.Check(respStream.Context(), namespaceSpec, reqJobs, observers); err != nil {
		return status.Errorf(codes.Internal, "failed to compile jobs\n%s", err.Error())
	}
	return nil
}

func (sv *JobSpecServiceServer) CreateJobSpecification(ctx context.Context, req *pb.CreateJobSpecificationRequest) (*pb.CreateJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpec, err := FromJobProto(req.GetSpec(), sv.pluginRepo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot deserialize job: \n%s", err.Error())
	}

	// validate job spec
	if err = sv.jobSvc.Check(ctx, namespaceSpec, []models.JobSpec{jobSpec}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "spec validation failed\n%s", err.Error())
	}

	err = sv.jobSvc.Create(ctx, namespaceSpec, jobSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to save job %s", err.Error(), jobSpec.Name)
	}

	if err := sv.jobSvc.Sync(ctx, namespaceSpec, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to sync jobs: \n%s", err.Error())
	}

	runtimeDeployJobSpecificationCounter.Inc()
	return &pb.CreateJobSpecificationResponse{
		Success: true,
		Message: fmt.Sprintf("job %s is created and deployed successfully on project %s", jobSpec.Name, req.GetProjectName()),
	}, nil
}

func (sv *JobSpecServiceServer) GetJobSpecification(ctx context.Context, req *pb.GetJobSpecificationRequest) (*pb.GetJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpec, err := sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: error while finding the job %s", err.Error(), req.GetJobName())
	}

	jobSpecAdapt := ToJobProto(jobSpec)

	return &pb.GetJobSpecificationResponse{
		Spec: jobSpecAdapt,
	}, nil
}

func (sv *JobSpecServiceServer) DeleteJobSpecification(ctx context.Context, req *pb.DeleteJobSpecificationRequest) (*pb.DeleteJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpecToDelete, err := sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: job %s does not exist", err.Error(), req.GetJobName())
	}

	if err := sv.jobSvc.Delete(ctx, namespaceSpec, jobSpecToDelete); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to delete job %s", err.Error(), req.GetJobName())
	}

	return &pb.DeleteJobSpecificationResponse{
		Success: true,
		Message: fmt.Sprintf("job %s has been deleted", jobSpecToDelete.Name),
	}, nil
}

func (sv *JobSpecServiceServer) RefreshJobs(req *pb.RefreshJobsRequest, respStream pb.JobSpecificationService_RefreshJobsServer) error {
	startTime := time.Now()

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobRefreshObserver{
		stream: respStream,
		log:    sv.l,
		mu:     new(sync.Mutex),
	})

	err := sv.jobSvc.Refresh(respStream.Context(), req.ProjectName, req.NamespaceNames, req.JobNames, observers)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to refresh jobs: \n%s", err.Error())
	}

	sv.l.Info("finished job refresh", "time", time.Since(startTime))
	return nil
}

func (sv *JobSpecServiceServer) GetDeployJobsStatus(ctx context.Context, req *pb.GetDeployJobsStatusRequest) (*pb.GetDeployJobsStatusResponse, error) {
	deployID, err := uuid.Parse(req.DeployId)
	if err != nil {
		return nil, err
	}

	jobDeployment, err := sv.jobSvc.GetDeployment(ctx, models.DeploymentID(deployID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get job deployment: \n%s", err.Error())
	}

	switch jobDeployment.Status {
	case models.JobDeploymentStatusSucceed:
		return &pb.GetDeployJobsStatusResponse{
			Status:       jobDeployment.Status.String(),
			SuccessCount: int32(jobDeployment.Details.SuccessCount),
		}, nil
	case models.JobDeploymentStatusFailed:
		var deployJobFailures []*pb.DeployJobFailure
		for _, failure := range jobDeployment.Details.Failures {
			deployJobFailures = append(deployJobFailures, &pb.DeployJobFailure{JobName: failure.JobName, Message: failure.Message})
		}

		return &pb.GetDeployJobsStatusResponse{
			Status:       jobDeployment.Status.String(),
			SuccessCount: int32(jobDeployment.Details.SuccessCount),
			FailureCount: int32(jobDeployment.Details.FailureCount),
			Failures:     deployJobFailures,
		}, nil
	default:
		return &pb.GetDeployJobsStatusResponse{
			Status: jobDeployment.Status.String(),
		}, nil
	}
}

func NewJobSpecServiceServer(l log.Logger, jobService models.JobService, pluginRepo models.PluginRepository,
	projectService service.ProjectService, namespaceService service.NamespaceService, progressObserver progress.Observer) *JobSpecServiceServer {
	return &JobSpecServiceServer{
		l:                l,
		jobSvc:           jobService,
		pluginRepo:       pluginRepo,
		projectService:   projectService,
		namespaceService: namespaceService,
		progressObserver: progressObserver,
	}
}
