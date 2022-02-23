package v1beta1

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/google/uuid"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	runtimeDeployJobSpecificationCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "runtime_deploy_jobspec",
		Help: "Number of jobs requested for deployment by runtime",
	})
	runtimeDeployResourceSpecificationCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "runtime_deploy_resourcespec",
		Help: "Number of resources requested for deployment by runtime",
	})
)

type JobEventService interface {
	Register(context.Context, models.NamespaceSpec, models.JobSpec, models.JobEvent) error
}

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) (*pb.JobSpecification, error)

	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification

	FromNamespaceProto(specification *pb.NamespaceSpecification) models.NamespaceSpec
	ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification

	ToInstanceProto(models.InstanceSpec) (*pb.InstanceSpec, error)

	FromResourceProto(res *pb.ResourceSpecification, storeName string) (models.ResourceSpec, error)
	ToResourceProto(res models.ResourceSpec) (*pb.ResourceSpecification, error)

	ToReplayExecutionTreeNode(res *tree.TreeNode) (*pb.ReplayExecutionTreeNode, error)
	ToReplayStatusTreeNode(res *tree.TreeNode) (*pb.ReplayStatusTreeNode, error)
}

type RuntimeServiceServer struct {
	version          string
	jobSvc           models.JobService
	jobEventSvc      JobEventService
	resourceSvc      models.DatastoreService
	adapter          ProtoAdapter
	projectService   service.ProjectService
	namespaceService service.NamespaceService
	secretService    service.SecretService
	runSvc           models.RunService
	scheduler        models.SchedulerUnit
	l                log.Logger

	progressObserver progress.Observer
	Now              func() time.Time

	pb.UnimplementedRuntimeServiceServer
}

func (sv *RuntimeServiceServer) Version(_ context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) {
	sv.l.Info("client requested for ping", "version", version.Client)
	response := &pb.VersionResponse{
		Server: sv.version,
	}
	return response, nil
}

func (sv *RuntimeServiceServer) GetJobTask(ctx context.Context, req *pb.GetJobTaskRequest) (*pb.GetJobTaskResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(err, "unable to get namespace")
	}

	jobSpec, err := sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: error while finding the job %s", err.Error(), req.GetJobName())
	}

	unitInfo := jobSpec.Task.Unit.Info()
	jobTaskSpec := &pb.JobTask{
		Name:         unitInfo.Name,
		Description:  unitInfo.Description,
		Image:        unitInfo.Image,
		Dependencies: nil,
		Destination:  nil,
	}
	if jobSpec.Task.Unit.DependencyMod != nil {
		taskDestination, taskDependencies, err := sv.jobSvc.GetTaskDependencies(ctx, namespaceSpec, jobSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: GetTaskDependencies", err.Error())
		}
		jobTaskSpec.Destination = &pb.JobTask_Destination{
			Destination: taskDestination.Destination,
			Type:        taskDestination.Type.String(),
		}
		for _, dep := range taskDependencies {
			jobTaskSpec.Dependencies = append(jobTaskSpec.Dependencies, &pb.JobTask_Dependency{
				Dependency: dep,
			})
		}
	}

	return &pb.GetJobTaskResponse{
		Task: jobTaskSpec,
	}, nil
}

// RegisterInstance creates a new job run and a running instance in persistent
// store then returns the config/assets attached to the job spec used in running
// the instance.
// Keep in mind, this whole operation should be in a single transaction
// if we expect multiple request coming for the same instance at the
// same time but that should never be the case in our use cases that's why
// for performance reasons we are choosing not to do so.
func (sv *RuntimeServiceServer) RegisterInstance(ctx context.Context, req *pb.RegisterInstanceRequest) (*pb.RegisterInstanceResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(err, "not able to find project")
	}

	instanceType, err := models.ToInstanceType(utils.FromEnumProto(req.InstanceType.String(), "TYPE"))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: instance type %s not found", err.Error(), req.InstanceType.String())
	}

	var namespaceSpec models.NamespaceSpec
	var jobRun models.JobRun
	if req.JobrunId == "" {
		var jobSpec models.JobSpec
		// a scheduled trigger instance, extract job run id if already present or create a new run
		jobSpec, namespaceSpec, err = sv.jobSvc.GetByNameForProject(ctx, req.GetJobName(), projSpec)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: job %s not found", err.Error(), req.GetJobName())
		}

		jobRun, err = sv.runSvc.GetScheduledRun(ctx, namespaceSpec, jobSpec, req.GetScheduledAt().AsTime())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to initialize scheduled run of job %s", err.Error(), req.GetJobName())
		}
	} else {
		// must be manual triggered job run
		jobRunID, err := uuid.Parse(req.JobrunId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s: failed to parse uuid of job %s", err.Error(), req.JobrunId)
		}
		jobRun, namespaceSpec, err = sv.runSvc.GetByID(ctx, jobRunID)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: failed to find scheduled run of job %s", err.Error(), req.JobrunId)
		}
	}

	instance, err := sv.runSvc.Register(ctx, namespaceSpec, jobRun, instanceType, req.GetInstanceName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to register instance of jobrun %s", err.Error(), jobRun)
	}
	envMap, fileMap, err := sv.runSvc.Compile(ctx, namespaceSpec, jobRun, instance)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to compile instance of job %s", err.Error(), req.GetJobName())
	}

	jobProto, err := sv.adapter.ToJobProto(jobRun.Spec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: cannot adapt job %s", err.Error(), jobRun.Spec.Name)
	}
	instanceProto, err := sv.adapter.ToInstanceProto(instance)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: cannot adapt instance for job %s", err.Error(), jobRun.Spec.Name)
	}
	return &pb.RegisterInstanceResponse{
		Project:   sv.adapter.ToProjectProto(projSpec),
		Job:       jobProto,
		Instance:  instanceProto,
		Namespace: sv.adapter.ToNamespaceProto(namespaceSpec),
		Context: &pb.InstanceContext{
			Envs:  envMap,
			Files: fileMap,
		},
	}, nil
}

func (sv *RuntimeServiceServer) JobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(err, "not able to find project")
	}

	_, _, err = sv.jobSvc.GetByNameForProject(ctx, req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to find the job %s for project %s", err.Error(),
			req.GetJobName(), req.GetProjectName())
	}

	jobStatuses, err := sv.scheduler.GetJobStatus(ctx, projSpec, req.GetJobName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to fetch jobStatus %s", err.Error(),
			req.GetJobName())
	}

	var adaptedJobStatus []*pb.JobStatus
	for _, jobStatus := range jobStatuses {
		ts := timestamppb.New(jobStatus.ScheduledAt)
		adaptedJobStatus = append(adaptedJobStatus, &pb.JobStatus{
			State:       jobStatus.State.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobStatusResponse{
		Statuses: adaptedJobStatus,
	}, nil
}

func (sv *RuntimeServiceServer) RegisterJobEvent(ctx context.Context, req *pb.RegisterJobEventRequest) (*pb.RegisterJobEventResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(err, "unable to get namespace")
	}

	jobSpec, err := sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: failed to find the job %s for namespace %s", err.Error(),
			req.GetJobName(), req.GetNamespaceName())
	}

	if req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing required job event values")
	}

	eventValues := map[string]*structpb.Value{}
	if req.GetEvent().Value != nil {
		eventValues = req.GetEvent().Value.GetFields()
	}
	if err := sv.jobEventSvc.Register(ctx, namespaceSpec, jobSpec, models.JobEvent{
		Type:  models.JobEventType(utils.FromEnumProto(req.GetEvent().Type.String(), "TYPE")),
		Value: eventValues,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register event: \n%s", err.Error())
	}

	return &pb.RegisterJobEventResponse{}, nil
}

func (sv *RuntimeServiceServer) GetWindow(ctx context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	scheduledTime := req.ScheduledAt.AsTime()
	err := req.ScheduledAt.CheckValid()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse schedule time %s", err.Error(), req.GetScheduledAt())
	}

	if req.GetSize() == "" || req.GetOffset() == "" || req.GetTruncateTo() == "" {
		return nil, status.Error(codes.InvalidArgument, "window size, offset and truncate_to must be provided")
	}

	window, err := prepareWindow(req.GetSize(), req.GetOffset(), req.GetTruncateTo())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	windowStart := timestamppb.New(window.GetStart(scheduledTime))
	windowEnd := timestamppb.New(window.GetEnd(scheduledTime))

	return &pb.GetWindowResponse{
		Start: windowStart,
		End:   windowEnd,
	}, nil
}

func (sv *RuntimeServiceServer) RunJob(ctx context.Context, req *pb.RunJobRequest) (*pb.RunJobResponse, error) {
	// create job run in db
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(err, "unable to get namespace")
	}

	var jobSpecs []models.JobSpec
	for _, jobSpecProto := range req.Specifications {
		jobSpec, err := sv.adapter.FromJobProto(jobSpecProto)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: cannot adapt job %s", err.Error(), jobSpecProto.GetName())
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}
	if err := sv.jobSvc.Run(ctx, namespaceSpec, jobSpecs, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to queue job for execution %s", err.Error(), req.ProjectName)
	}

	return &pb.RunJobResponse{}, nil
}

func NewRuntimeServiceServer(
	l log.Logger,
	version string,
	jobSvc models.JobService,
	jobEventService JobEventService,
	datastoreSvc models.DatastoreService,
	projectService service.ProjectService,
	namespaceService service.NamespaceService,
	secretService service.SecretService,
	adapter ProtoAdapter,
	progressObserver progress.Observer,
	instSvc models.RunService,
	scheduler models.SchedulerUnit,
) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		l:                l,
		version:          version,
		jobSvc:           jobSvc,
		jobEventSvc:      jobEventService,
		resourceSvc:      datastoreSvc,
		adapter:          adapter,
		progressObserver: progressObserver,
		runSvc:           instSvc,
		scheduler:        scheduler,
		secretService:    secretService,
		namespaceService: namespaceService,
		projectService:   projectService,
	}
}
