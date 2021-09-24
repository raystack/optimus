package v1

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/meta"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

type SecretRepoFactory interface {
	New(spec models.ProjectSpec) store.ProjectSecretRepository
}

type JobEventService interface {
	Register(context.Context, models.NamespaceSpec, models.JobSpec, models.JobEvent) error
}

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) (*pb.JobSpecification, error)

	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
	ToProjectProtoWithSecret(proj models.ProjectSpec, pType models.InstanceType, pName string) *pb.ProjectSpecification

	FromNamespaceProto(specification *pb.NamespaceSpecification) models.NamespaceSpec
	ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification

	FromInstanceProto(*pb.InstanceSpec) (models.InstanceSpec, error)
	ToInstanceProto(models.InstanceSpec) (*pb.InstanceSpec, error)

	FromResourceProto(res *pb.ResourceSpecification, storeName string) (models.ResourceSpec, error)
	ToResourceProto(res models.ResourceSpec) (*pb.ResourceSpecification, error)

	ToReplayExecutionTreeNode(res *tree.TreeNode) (*pb.ReplayExecutionTreeNode, error)
	ToReplayStatusTreeNode(res *tree.TreeNode) (*pb.ReplayStatusTreeNode, error)
}

type RuntimeServiceServer struct {
	version              string
	jobSvc               models.JobService
	jobEventSvc          JobEventService
	resourceSvc          models.DatastoreService
	adapter              ProtoAdapter
	projectRepoFactory   ProjectRepoFactory
	namespaceRepoFactory NamespaceRepoFactory
	secretRepoFactory    SecretRepoFactory
	runSvc               models.RunService
	scheduler            models.SchedulerUnit
	l                    log.Logger

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

func (sv *RuntimeServiceServer) DeployJobSpecification(req *pb.DeployJobSpecificationRequest, respStream pb.RuntimeService_DeployJobSpecificationServer) error {
	startTime := time.Now()

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	var jobsToKeep []models.JobSpec
	for _, reqJob := range req.GetJobs() {
		adaptJob, err := sv.adapter.FromJobProto(reqJob)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: cannot adapt job %s", err.Error(), reqJob.GetName())
		}

		err = sv.jobSvc.Create(namespaceSpec, adaptJob)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: failed to save %s", err.Error(), adaptJob.Name)
		}
		jobsToKeep = append(jobsToKeep, adaptJob)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobSyncObserver{
		stream: respStream,
		log:    sv.l,
		mu:     new(sync.Mutex),
	})

	// delete specs not sent for deployment from internal repository
	if err := sv.jobSvc.KeepOnly(namespaceSpec, jobsToKeep, observers); err != nil {
		return status.Errorf(codes.Internal, "failed to delete jobs: \n%s", err.Error())
	}

	if err := sv.jobSvc.Sync(respStream.Context(), namespaceSpec, observers); err != nil {
		return status.Errorf(codes.Internal, "failed to sync jobs: \n%s", err.Error())
	}

	sv.l.Info("finished job deployment", "time", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) ListJobSpecification(ctx context.Context, req *pb.ListJobSpecificationRequest) (*pb.ListJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	jobSpecs, err := sv.jobSvc.GetAll(namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to retrieve jobs for project %s", err.Error(), req.GetProjectName())
	}

	jobProtos := []*pb.JobSpecification{}
	for _, jobSpec := range jobSpecs {
		jobProto, err := sv.adapter.ToJobProto(jobSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to parse job spec %s", err.Error(), jobSpec.Name)
		}
		jobProtos = append(jobProtos, jobProto)
	}
	return &pb.ListJobSpecificationResponse{
		Jobs: jobProtos,
	}, nil
}

func (sv *RuntimeServiceServer) DumpJobSpecification(ctx context.Context, req *pb.DumpJobSpecificationRequest) (*pb.DumpJobSpecificationResponse, error) {
	return &pb.DumpJobSpecificationResponse{}, status.Error(codes.Unimplemented, "disabled at the moment")
}

func (sv *RuntimeServiceServer) CheckJobSpecification(ctx context.Context, req *pb.CheckJobSpecificationRequest) (*pb.CheckJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	j, err := sv.adapter.FromJobProto(req.GetJob())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to adapt job %s", err.Error(), req.GetJob().Name)
	}
	reqJobs := []models.JobSpec{j}

	if err = sv.jobSvc.Check(ctx, namespaceSpec, reqJobs, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to compile jobs\n%s", err.Error())
	}
	return &pb.CheckJobSpecificationResponse{Success: true}, nil
}

func (sv *RuntimeServiceServer) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, respStream pb.RuntimeService_CheckJobSpecificationsServer) error {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
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
		j, err := sv.adapter.FromJobProto(jobProto)
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

func (sv *RuntimeServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projectSpec := sv.adapter.FromProjectProto(req.GetProject())

	if err := projectRepo.Save(projectSpec); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to save project %s", err.Error(), req.GetProject().GetName())
	}

	if req.GetNamespace() != nil {
		savedProjectSpec, err := projectRepo.GetByName(projectSpec.Name)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: failed to find project %s",
				err.Error(), req.GetProject().GetName())
		}

		namespaceRepo := sv.namespaceRepoFactory.New(savedProjectSpec)
		namespaceSpec := sv.adapter.FromNamespaceProto(req.GetNamespace())
		if err = namespaceRepo.Save(namespaceSpec); err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to save project %s with namespace %s",
				err.Error(), req.GetProject().GetName(), req.GetNamespace().GetName())
		}
	}

	return &pb.RegisterProjectResponse{
		Success: true,
		Message: "saved successfully",
	}, nil
}

func (sv *RuntimeServiceServer) RegisterProjectNamespace(ctx context.Context, req *pb.RegisterProjectNamespaceRequest) (*pb.RegisterProjectNamespaceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceSpec := sv.adapter.FromNamespaceProto(req.GetNamespace())
	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	if err = namespaceRepo.Save(namespaceSpec); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to save namespace %s for project %s", err.Error(), namespaceSpec.Name, projSpec.Name)
	}

	return &pb.RegisterProjectNamespaceResponse{
		Success: true,
		Message: "saved successfully",
	}, nil
}

func (sv *RuntimeServiceServer) CreateJobSpecification(ctx context.Context, req *pb.CreateJobSpecificationRequest) (*pb.CreateJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace())
	}

	jobSpec, err := sv.adapter.FromJobProto(req.GetSpec())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot deserialize job: \n%s", err.Error())
	}

	// validate job spec
	if err = sv.jobSvc.Check(ctx, namespaceSpec, []models.JobSpec{jobSpec}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "spec validation failed\n%s", err.Error())
	}

	err = sv.jobSvc.Create(namespaceSpec, jobSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to save job %s", err.Error(), jobSpec.Name)
	}

	if err := sv.jobSvc.Sync(ctx, namespaceSpec, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to sync jobs: \n%s", err.Error())
	}

	return &pb.CreateJobSpecificationResponse{
		Success: true,
		Message: fmt.Sprintf("job %s is created and deployed successfully on project %s", jobSpec.Name, req.GetProjectName()),
	}, nil
}

func (sv *RuntimeServiceServer) ReadJobSpecification(ctx context.Context, req *pb.ReadJobSpecificationRequest) (*pb.ReadJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace())
	}

	jobSpec, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: error while finding the job %s", err.Error(), req.GetJobName())
	}

	jobSpecAdapt, err := sv.adapter.ToJobProto(jobSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot serialize job: \n%s", err.Error())
	}

	return &pb.ReadJobSpecificationResponse{
		Spec: jobSpecAdapt,
	}, nil
}

func (sv *RuntimeServiceServer) DeleteJobSpecification(ctx context.Context, req *pb.DeleteJobSpecificationRequest) (*pb.DeleteJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace())
	}

	jobSpecToDelete, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
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

func (sv *RuntimeServiceServer) ListProjects(ctx context.Context, req *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projects, err := projectRepo.GetAll()
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to retrieve saved projects: \n%s", err.Error())
	}

	projSpecsProto := []*pb.ProjectSpecification{}
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, sv.adapter.ToProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		Projects: projSpecsProto,
	}, nil
}

func (sv *RuntimeServiceServer) ListProjectNamespaces(ctx context.Context, req *pb.ListProjectNamespacesRequest) (*pb.ListProjectNamespacesResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpecs, err := namespaceRepo.GetAll()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while fetching namespaces: \n%s", err.Error())
	}

	namespaceSpecsProto := []*pb.NamespaceSpecification{}
	for _, namespace := range namespaceSpecs {
		namespaceSpecsProto = append(namespaceSpecsProto, sv.adapter.ToNamespaceProto(namespace))
	}

	return &pb.ListProjectNamespacesResponse{
		Namespaces: namespaceSpecsProto,
	}, nil
}

func (sv *RuntimeServiceServer) RegisterInstance(ctx context.Context, req *pb.RegisterInstanceRequest) (*pb.RegisterInstanceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	instanceType, err := models.InstanceType("").New(req.InstanceType.String())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: instance type %s not found", err.Error(), req.InstanceType.String())
	}

	var namespaceSpec models.NamespaceSpec
	var jobRun models.JobRun
	if req.JobrunId == "" {
		var jobSpec models.JobSpec
		// a scheduled trigger instance, extract job run id if already present or create a new run
		jobSpec, namespaceSpec, err = sv.jobSvc.GetByNameForProject(req.GetJobName(), projSpec)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: job %s not found", err.Error(), req.GetJobName())
		}

		jobRun, err = sv.runSvc.GetScheduledRun(namespaceSpec, jobSpec, req.GetScheduledAt().AsTime())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to initialize scheduled run of job %s", err.Error(), req.GetJobName())
		}
	} else {
		// must be manual triggered job run
		jobRunID, err := uuid.Parse(req.JobrunId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s: failed to parse uuid of job %s", err.Error(), req.JobrunId)
		}
		jobRun, namespaceSpec, err = sv.runSvc.GetByID(jobRunID)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: failed to find scheduled run of job %s", err.Error(), req.JobrunId)
		}
	}

	instance, err := sv.runSvc.Register(namespaceSpec, jobRun, instanceType, req.GetInstanceName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to register instance of jobrun %s", err.Error(), jobRun)
	}
	envMap, fileMap, err := sv.runSvc.Compile(namespaceSpec, jobRun, instance)
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
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	_, _, err = sv.jobSvc.GetByNameForProject(req.GetJobName(), projSpec)
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
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	jobSpec, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: failed to find the job %s for namespace %s", err.Error(),
			req.GetJobName(), req.GetNamespace())
	}

	if req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing required job event values")
	}

	eventValues := map[string]*structpb.Value{}
	if req.GetEvent().Value != nil {
		eventValues = req.GetEvent().Value.GetFields()
	}
	if err := sv.jobEventSvc.Register(ctx, namespaceSpec, jobSpec, models.JobEvent{
		Type:  models.JobEventType(strings.ToLower(req.GetEvent().Type.String())),
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

func (sv *RuntimeServiceServer) RegisterSecret(ctx context.Context, req *pb.RegisterSecretRequest) (*pb.RegisterSecretResponse, error) {
	if req.GetValue() == "" {
		return nil, status.Error(codes.Internal, "empty value for secret")
	}
	// decode base64
	base64Decoded, err := base64.StdEncoding.DecodeString(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to decode base64 string: \n%s", err.Error())
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	secretRepo := sv.secretRepoFactory.New(projSpec)
	if err := secretRepo.Save(models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: string(base64Decoded),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to save secret %s", err.Error(), req.GetSecretName())
	}

	return &pb.RegisterSecretResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	return &pb.CreateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	return &pb.UpdateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	response, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	protoResource, err := sv.adapter.ToResourceProto(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to adapt resource %s", err.Error(), req.ResourceName)
	}

	return &pb.ReadResourceResponse{
		Success:  true,
		Resource: protoResource,
	}, nil
}

func (sv *RuntimeServiceServer) DeployResourceSpecification(req *pb.DeployResourceSpecificationRequest, respStream pb.RuntimeService_DeployResourceSpecificationServer) error {
	startTime := time.Now()

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	var resourceSpecs []models.ResourceSpec
	for _, resourceProto := range req.GetResources() {
		adapted, err := sv.adapter.FromResourceProto(resourceProto, req.DatastoreName)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: cannot adapt resource %s", err.Error(), resourceProto.GetName())
		}
		resourceSpecs = append(resourceSpecs, adapted)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&resourceObserver{
		stream: respStream,
		log:    sv.l,
		mu:     new(sync.Mutex),
	})

	if err := sv.resourceSvc.UpdateResource(respStream.Context(), namespaceSpec, resourceSpecs, observers); err != nil {
		return status.Errorf(codes.Internal, "failed to update resources: \n%s", err.Error())
	}
	sv.l.Info("finished resource deployment in", "time", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespace())
	}

	resourceSpecs, err := sv.resourceSvc.GetAll(namespaceSpec, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to retrieve jobs for project %s", err.Error(), req.GetProjectName())
	}

	resourceProtos := []*pb.ResourceSpecification{}
	for _, resourceSpec := range resourceSpecs {
		resourceProto, err := sv.adapter.ToResourceProto(resourceSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to parse resource spec %s", err.Error(), resourceSpec.Name)
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}
	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}

func (sv *RuntimeServiceServer) ReplayDryRun(ctx context.Context, req *pb.ReplayDryRunRequest) (*pb.ReplayDryRunResponse, error) {
	replayRequest, err := sv.parseReplayRequest(req.ProjectName, req.Namespace, req.JobName, req.StartDate, req.EndDate, false)
	if err != nil {
		return nil, err
	}

	rootNode, err := sv.jobSvc.ReplayDryRun(ctx, replayRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while processing replay dry run: %v", err)
	}

	node, err := sv.adapter.ToReplayExecutionTreeNode(rootNode)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while preparing replay dry run response: %v", err)
	}
	return &pb.ReplayDryRunResponse{
		Success:  true,
		Response: node,
	}, nil
}

func (sv *RuntimeServiceServer) Replay(ctx context.Context, req *pb.ReplayRequest) (*pb.ReplayResponse, error) {
	replayWorkerRequest, err := sv.parseReplayRequest(req.ProjectName, req.Namespace, req.JobName, req.StartDate, req.EndDate, req.Force)
	if err != nil {
		return nil, err
	}

	replayUUID, err := sv.jobSvc.Replay(ctx, replayWorkerRequest)
	if err != nil {
		if errors.Is(err, job.ErrRequestQueueFull) {
			return nil, status.Errorf(codes.Unavailable, "error while processing replay: %v", err)
		} else if errors.Is(err, job.ErrConflictedJobRun) {
			return nil, status.Errorf(codes.FailedPrecondition, "error while validating replay: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "error while processing replay: %v", err)
	}

	return &pb.ReplayResponse{
		Id: replayUUID,
	}, nil
}

func (sv *RuntimeServiceServer) GetReplayStatus(ctx context.Context, req *pb.GetReplayStatusRequest) (*pb.GetReplayStatusResponse, error) {
	replayRequest, err := sv.parseReplayStatusRequest(req)
	if err != nil {
		return nil, err
	}

	replayState, err := sv.jobSvc.GetReplayStatus(ctx, replayRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay: %v", err)
	}

	node, err := sv.adapter.ToReplayStatusTreeNode(replayState.Node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay status tree: %v", err)
	}

	return &pb.GetReplayStatusResponse{
		State:    replayState.Status,
		Response: node,
	}, nil
}

func (sv *RuntimeServiceServer) parseReplayStatusRequest(req *pb.GetReplayStatusRequest) (models.ReplayRequest, error) {
	projSpec, err := sv.getProjectSpec(req.ProjectName)
	if err != nil {
		return models.ReplayRequest{}, err
	}

	uuid, err := uuid.Parse(req.Id)
	if err != nil {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "error while parsing replay ID: %v", err)
	}

	replayRequest := models.ReplayRequest{
		ID:      uuid,
		Project: projSpec,
	}
	return replayRequest, nil
}

func (sv *RuntimeServiceServer) ListReplays(ctx context.Context, req *pb.ListReplaysRequest) (*pb.ListReplaysResponse, error) {
	projSpec, err := sv.getProjectSpec(req.ProjectName)
	if err != nil {
		return nil, err
	}

	replays, err := sv.jobSvc.GetReplayList(projSpec.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay list: %v", err)
	}

	var replaySpecs []*pb.ReplaySpec
	for _, replaySpec := range replays {
		replaySpecs = append(replaySpecs, &pb.ReplaySpec{
			Id:        replaySpec.ID.String(),
			JobName:   replaySpec.Job.GetName(),
			StartDate: timestamppb.New(replaySpec.StartDate),
			EndDate:   timestamppb.New(replaySpec.EndDate),
			State:     replaySpec.Status,
			CreatedAt: timestamppb.New(replaySpec.CreatedAt),
		})
	}

	return &pb.ListReplaysResponse{
		ReplayList: replaySpecs,
	}, nil
}

func (sv *RuntimeServiceServer) parseReplayRequest(projectName string, namespace string, jobName string, startDate string,
	endDate string, forceFlag bool) (models.ReplayRequest, error) {
	projSpec, err := sv.getProjectSpec(projectName)
	if err != nil {
		return models.ReplayRequest{}, err
	}

	jobSpec, err := sv.getJobSpec(projSpec, namespace, jobName)
	if err != nil {
		return models.ReplayRequest{}, err
	}

	windowStart, err := time.Parse(job.ReplayDateFormat, startDate)
	if err != nil {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "unable to parse replay start date(e.g. %s): %v", job.ReplayDateFormat, err)
	}

	windowEnd := windowStart
	if endDate != "" {
		if windowEnd, err = time.Parse(job.ReplayDateFormat, endDate); err != nil {
			return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "unable to parse replay end date(e.g. %s): %v", job.ReplayDateFormat, err)
		}
	}
	if windowEnd.Before(windowStart) {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "replay end date cannot be before start date")
	}
	replayRequest := models.ReplayRequest{
		Job:     jobSpec,
		Start:   windowStart,
		End:     windowEnd,
		Project: projSpec,
		Force:   forceFlag,
	}
	return replayRequest, nil
}

func (sv *RuntimeServiceServer) getProjectSpec(projectName string) (models.ProjectSpec, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(projectName)
	if err != nil {
		return models.ProjectSpec{}, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), projectName)
	}
	return projSpec, nil
}

func (sv *RuntimeServiceServer) getJobSpec(projSpec models.ProjectSpec, namespace string, jobName string) (models.JobSpec, error) {
	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(namespace)
	if err != nil {
		return models.JobSpec{}, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), namespace)
	}

	jobSpec, err := sv.jobSvc.GetByName(jobName, namespaceSpec)
	if err != nil {
		return models.JobSpec{}, status.Errorf(codes.NotFound, "%s: failed to find the job %s for namespace %s", err.Error(),
			jobName, namespace)
	}
	return jobSpec, nil
}

func (sv *RuntimeServiceServer) BackupDryRun(ctx context.Context, req *pb.BackupDryRunRequest) (*pb.BackupDryRunResponse, error) {
	projectSpec, err := sv.getProjectSpec(req.ProjectName)
	if err != nil {
		return nil, err
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projectSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.Namespace)
	}

	resourceSpec, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	var jobSpecs []models.JobSpec
	jobSpec, err := sv.jobSvc.GetByDestination(projectSpec, resourceSpec.URN)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting job: %v", err)
	}
	jobSpecs = append(jobSpecs, jobSpec)

	if !req.IgnoreDownstream {
		downstreamSpecs, err := sv.jobSvc.GetDownstream(ctx, projectSpec, jobSpec.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error while getting job downstream: %v", err)
		}
		jobSpecs = append(jobSpecs, downstreamSpecs...)
	}

	//should add config
	backupRequest := models.BackupRequest{
		ResourceName:     req.ResourceName,
		Project:          projectSpec,
		Namespace:        namespaceSpec,
		Description:      req.Description,
		IgnoreDownstream: req.IgnoreDownstream,
		DryRun:           true,
	}
	resourcesToBackup, err := sv.resourceSvc.BackupResourceDryRun(ctx, backupRequest, jobSpecs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while doing backup dry run: %v", err)
	}

	return &pb.BackupDryRunResponse{
		ResourceName: resourcesToBackup,
	}, nil
}

func (sv *RuntimeServiceServer) Backup(ctx context.Context, req *pb.BackupRequest) (*pb.BackupResponse, error) {
	projectSpec, err := sv.getProjectSpec(req.ProjectName)
	if err != nil {
		return nil, err
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projectSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.Namespace)
	}

	resourceSpec, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	var jobSpecs []models.JobSpec
	jobSpec, err := sv.jobSvc.GetByDestination(projectSpec, resourceSpec.URN)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting job: %v", err)
	}
	jobSpecs = append(jobSpecs, jobSpec)

	if !req.IgnoreDownstream {
		downstreamSpecs, err := sv.jobSvc.GetDownstream(projectSpec, jobSpec.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error while getting job downstream: %v", err)
		}
		jobSpecs = append(jobSpecs, downstreamSpecs...)
	}

	backupRequest := models.BackupRequest{
		ResourceName:     req.ResourceName,
		Project:          projectSpec,
		Namespace:        namespaceSpec,
		Description:      req.Description,
		IgnoreDownstream: req.IgnoreDownstream,
		DryRun:           false,
		Config:           req.Config,
		BackupTime:       time.Now(),
	}
	results, err := sv.resourceSvc.BackupResource(ctx, backupRequest, jobSpecs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while doing backup: %v", err)
	}

	return &pb.BackupResponse{
		Urn: results,
	}, nil
}

func (sv *RuntimeServiceServer) RunJob(ctx context.Context, req *pb.RunJobRequest) (*pb.RunJobResponse, error) {
	// create job run in db
	projSpec, err := sv.projectRepoFactory.New().GetByName(req.ProjectName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.ProjectName)
	}

	namespaceSpec, err := sv.namespaceRepoFactory.New(projSpec).GetByName(req.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.Namespace)
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
	projectRepoFactory ProjectRepoFactory,
	namespaceRepoFactory NamespaceRepoFactory,
	secretRepoFactory SecretRepoFactory,
	adapter ProtoAdapter,
	progressObserver progress.Observer,
	instSvc models.RunService,
	scheduler models.SchedulerUnit,
) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		l:                    l,
		version:              version,
		jobSvc:               jobSvc,
		jobEventSvc:          jobEventService,
		resourceSvc:          datastoreSvc,
		adapter:              adapter,
		projectRepoFactory:   projectRepoFactory,
		namespaceRepoFactory: namespaceRepoFactory,
		progressObserver:     progressObserver,
		runSvc:               instSvc,
		scheduler:            scheduler,
		secretRepoFactory:    secretRepoFactory,
	}
}

type jobSyncObserver struct {
	stream pb.RuntimeService_DeployJobSpecificationServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *jobSyncObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	switch evt := e.(type) {
	case *models.EventJobUpload:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			Ack:     true,
			JobName: evt.Name,
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Message = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send deploy spec ack", "evt", evt.String(), "error", err)
		}
	case *models.EventJobRemoteDelete:
		resp := &pb.DeployJobSpecificationResponse{
			JobName: evt.Name,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send delete notification", "evt", evt.String(), "error", err)
		}
	case *job.EventJobSpecUnknownDependencyUsed:
		resp := &pb.DeployJobSpecificationResponse{
			JobName: evt.Job,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send unknown dependency notification", "evt", evt.String(), "error", err)
		}
	case *meta.EventPublish:
		resp := &pb.DeployJobSpecificationResponse{
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send publish metadata notification", "evt", evt.String(), "error", err)
		}
	}
}

type resourceObserver struct {
	stream pb.RuntimeService_DeployResourceSpecificationServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *resourceObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	switch evt := e.(type) {
	case *datastore.EventResourceUpdated:
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
	stream pb.RuntimeService_CheckJobSpecificationsServer
	log    log.Logger
	mu     *sync.Mutex
}

func (obs *jobCheckObserver) Notify(e progress.Event) {
	obs.mu.Lock()
	defer obs.mu.Unlock()

	switch evt := e.(type) {
	case *job.EventJobCheckFailed:
		resp := &pb.CheckJobSpecificationsResponse{
			Success: false,
			Ack:     true,
			JobName: evt.Name,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error("failed to send check ack", "job name", evt.Name, "error", err)
		}
	case *job.EventJobCheckSuccess:
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
