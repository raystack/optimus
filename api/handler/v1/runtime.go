package v1

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/odpf/optimus/datastore"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/core/logger"
	log "github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

type JobRepoFactory interface {
	New(spec models.ProjectSpec) store.JobSpecRepository
}

type SecretRepoFactory interface {
	New(spec models.ProjectSpec) store.ProjectSecretRepository
}

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) (*pb.JobSpecification, error)

	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification

	FromNamespaceProto(specification *pb.NamespaceSpecification) models.NamespaceSpec
	ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification

	FromInstanceProto(*pb.InstanceSpec) (models.InstanceSpec, error)
	ToInstanceProto(models.InstanceSpec) (*pb.InstanceSpec, error)

	FromResourceProto(res *pb.ResourceSpecification, storeName string) (models.ResourceSpec, error)
	ToResourceProto(res models.ResourceSpec) (*pb.ResourceSpecification, error)
}

type RuntimeServiceServer struct {
	version              string
	jobSvc               models.JobService
	resourceSvc          models.DatastoreService
	adapter              ProtoAdapter
	projectRepoFactory   ProjectRepoFactory
	namespaceRepoFactory NamespaceRepoFactory
	secretRepoFactory    SecretRepoFactory
	instSvc              models.InstanceService
	scheduler            models.SchedulerUnit

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

func (sv *RuntimeServiceServer) DeployJobSpecification(req *pb.DeployJobSpecificationRequest, respStream pb.RuntimeService_DeployJobSpecificationServer) error {
	startTime := time.Now()

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	var jobsToKeep []models.JobSpec
	for _, reqJob := range req.GetJobs() {
		adaptJob, err := sv.adapter.FromJobProto(reqJob)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("%s: cannot adapt job %s", err.Error(), reqJob.GetName()))
		}

		err = sv.jobSvc.Create(namespaceSpec, adaptJob)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("%s: failed to save %s", err.Error(), adaptJob.Name))
		}
		jobsToKeep = append(jobsToKeep, adaptJob)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobSyncObserver{
		stream: respStream,
		log:    logrus.New(),
	})

	// delete specs not sent for deployment
	// currently we don't support deploying a single dag at a time so this will change
	// once we do that
	if err := sv.jobSvc.KeepOnly(namespaceSpec, jobsToKeep, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("%s: failed to delete jobs", err.Error()))
	}

	if err := sv.jobSvc.Sync(respStream.Context(), namespaceSpec, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("%s\nfailed to sync jobs", err.Error()))
	}

	logger.I("finished job deployment in", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) ListJobSpecification(ctx context.Context, req *pb.ListJobSpecificationRequest) (*pb.ListJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	jobSpecs, err := sv.jobSvc.GetAll(namespaceSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to retrive jobs for project %s", err.Error(), req.GetProjectName()))
	}

	jobProtos := []*pb.JobSpecification{}
	for _, jobSpec := range jobSpecs {
		jobProto, err := sv.adapter.ToJobProto(jobSpec)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse job spec %s", err.Error(), jobSpec.Name))
		}
		jobProtos = append(jobProtos, jobProto)
	}
	return &pb.ListJobSpecificationResponse{
		Jobs: jobProtos,
	}, nil
}

func (sv *RuntimeServiceServer) DumpJobSpecification(ctx context.Context, req *pb.DumpJobSpecificationRequest) (*pb.DumpJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	reqJobSpec, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: job %s not found", err.Error(), req.GetJobName()))
	}

	compiledJob, err := sv.jobSvc.Dump(namespaceSpec, reqJobSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to compile %s", err.Error(), reqJobSpec.Name))
	}

	return &pb.DumpJobSpecificationResponse{Success: true, Content: string(compiledJob.Contents)}, nil
}

func (sv *RuntimeServiceServer) CheckJobSpecification(ctx context.Context, req *pb.CheckJobSpecificationRequest) (*pb.CheckJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	j, err := sv.adapter.FromJobProto(req.GetJob())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to adapt job %s\n%s", req.GetJob().Name, err.Error()))
	}
	reqJobs := []models.JobSpec{j}

	if err = sv.jobSvc.Check(namespaceSpec, reqJobs, nil); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to compile jobs\n%s", err.Error()))
	}
	return &pb.CheckJobSpecificationResponse{Success: true}, nil
}

func (sv *RuntimeServiceServer) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, respStream pb.RuntimeService_CheckJobSpecificationsServer) error {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&jobCheckObserver{
		stream: respStream,
		log:    logrus.New(),
	})

	reqJobs := []models.JobSpec{}
	for _, jobProto := range req.GetJobs() {
		j, err := sv.adapter.FromJobProto(jobProto)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to adapt job %s\n%s", jobProto.Name, err.Error()))
		}
		reqJobs = append(reqJobs, j)
	}

	if err = sv.jobSvc.Check(namespaceSpec, reqJobs, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to compile jobs\n%s", err.Error()))
	}
	return nil
}

func (sv *RuntimeServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projectSpec := sv.adapter.FromProjectProto(req.GetProject())

	if err := projectRepo.Save(projectSpec); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save project %s", err.Error(), req.GetProject().GetName()))
	}

	if req.GetNamespace() != nil {
		savedProjectSpec, err := projectRepo.GetByName(projectSpec.Name)
		if err != nil {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: failed to find project %s",
				err.Error(), req.GetProject().GetName()))
		}

		namespaceRepo := sv.namespaceRepoFactory.New(savedProjectSpec)
		namespaceSpec := sv.adapter.FromNamespaceProto(req.GetNamespace())
		if err = namespaceRepo.Save(namespaceSpec); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save project %s with namespace %s",
				err.Error(), req.GetProject().GetName(), req.GetNamespace().GetName()))
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
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceSpec := sv.adapter.FromNamespaceProto(req.GetNamespace())
	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	if err = namespaceRepo.Save(namespaceSpec); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save namespace %s for project %s", err.Error(), namespaceSpec.Name, projSpec.Name))
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
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace()))
	}

	jobSpec, err := sv.adapter.FromJobProto(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: cannot deserialize job", err.Error()))
	}

	// validate job spec
	if err = sv.jobSvc.Check(namespaceSpec, []models.JobSpec{jobSpec}, sv.progressObserver); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("spec validation failed\n%s", err.Error()))
	}

	err = sv.jobSvc.Create(namespaceSpec, jobSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save job %s", err.Error(), jobSpec.Name))
	}

	if err := sv.jobSvc.Sync(ctx, namespaceSpec, sv.progressObserver); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s\nfailed to sync jobs", err.Error()))
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
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace()))
	}

	jobSpec, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: error while finding the job %s", err.Error(), req.GetJobName()))
	}

	jobSpecAdapt, err := sv.adapter.ToJobProto(jobSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: cannot serialize job", err.Error()))
	}

	return &pb.ReadJobSpecificationResponse{
		Spec: jobSpecAdapt,
	}, nil
}

func (sv *RuntimeServiceServer) DeleteJobSpecification(ctx context.Context, req *pb.DeleteJobSpecificationRequest) (*pb.DeleteJobSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found. Is it registered?", err.Error(), req.GetNamespace()))
	}

	jobSpecToDelete, err := sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: job %s does not exist", err.Error(), req.GetJobName()))
	}

	if err := sv.jobSvc.Delete(ctx, namespaceSpec, jobSpecToDelete); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to delete job %s", err.Error(), req.GetJobName()))
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
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: failed to retrive saved projects", err.Error()))
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
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpecs, err := namespaceRepo.GetAll()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: error while fetching namespaces", err.Error()))
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
	jobScheduledTime, err := ptypes.Timestamp(req.GetScheduledAt())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s: failed to parse schedule time of job %s", err.Error(), req.GetScheduledAt()))
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	jobSpec, namespaceSpec, err := sv.jobSvc.GetByNameForProject(req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: job %s not found", err.Error(), req.GetJobName()))
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
		Project:   sv.adapter.ToProjectProto(projSpec),
		Job:       jobProto,
		Instance:  instanceProto,
		Namespace: sv.adapter.ToNamespaceProto(namespaceSpec),
	}, nil
}

func (sv *RuntimeServiceServer) JobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	_, err = sv.jobSvc.GetByName(req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: failed to find the job %s for namespace %s", err.Error(),
			req.GetJobName(), req.GetNamespace()))
	}

	jobStatuses, err := sv.scheduler.GetJobStatus(ctx, projSpec, req.GetJobName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: failed to fetch jobStatus %s", err.Error(),
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
		Statuses: adaptedJobStatus,
	}, nil
}

func (sv *RuntimeServiceServer) GetWindow(ctx context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	scheduledTime, err := ptypes.Timestamp(req.GetScheduledAt())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse schedule time %s", err.Error(), req.GetScheduledAt()))
	}

	if req.GetSize() == "" || req.GetOffset() == "" || req.GetTruncateTo() == "" {
		return nil, status.Error(codes.InvalidArgument, "window size, offset and truncate_to must be provided")
	}

	window, err := prepareWindow(req.GetSize(), req.GetOffset(), req.GetTruncateTo())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	windowStart, err1 := ptypes.TimestampProto(window.GetStart(scheduledTime))
	windowEnd, err2 := ptypes.TimestampProto(window.GetEnd(scheduledTime))
	if err1 != nil || err2 != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to convert timestamp %s", err.Error(), scheduledTime))
	}

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
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s: failed to decode base64 string", err.Error()))
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	secretRepo := sv.secretRepoFactory.New(projSpec)
	if err := secretRepo.Save(models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: string(base64Decoded),
	}); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to save secret %s", err.Error(), req.GetSecretName()))
	}

	return &pb.RegisterSecretResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse resource %s", err.Error(), req.Resource.GetName()))
	}

	if err := sv.resourceSvc.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to create resource %s", err.Error(), req.Resource.GetName()))
	}
	return &pb.CreateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse resource %s", err.Error(), req.Resource.GetName()))
	}

	if err := sv.resourceSvc.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to create resource %s", err.Error(), req.Resource.GetName()))
	}
	return &pb.UpdateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())

	response, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to create resource %s", err.Error(), req.ResourceName))
	}

	protoResource, err := sv.adapter.ToResourceProto(response)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to adapt resource %s", err.Error(), req.ResourceName))
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
		return status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: namespace %s not found", err.Error(), req.GetNamespace()))
	}

	var resourceSpecs []models.ResourceSpec
	for _, resourceProto := range req.GetResources() {
		adapted, err := sv.adapter.FromResourceProto(resourceProto, req.DatastoreName)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("%s: cannot adapt resource %s", err.Error(), resourceProto.GetName()))
		}
		resourceSpecs = append(resourceSpecs, adapted)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&resourceObserver{
		stream: respStream,
		log:    logrus.New(),
	})

	if err := sv.resourceSvc.UpdateResource(respStream.Context(), namespaceSpec, resourceSpecs, observers); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to update resources:\n%s", err.Error()))
	}
	logger.I("finished resource deployment in", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProjectName())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("%s: project %s not found", err.Error(), req.GetProjectName()))
	}

	namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
	namespaceSpec, err := namespaceRepo.GetByName(req.GetNamespace())

	resourceSpecs, err := sv.resourceSvc.GetAll(namespaceSpec, req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to retrive jobs for project %s", err.Error(), req.GetProjectName()))
	}

	resourceProtos := []*pb.ResourceSpecification{}
	for _, resourceSpec := range resourceSpecs {
		resourceProto, err := sv.adapter.ToResourceProto(resourceSpec)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("%s: failed to parse job spec %s", err.Error(), resourceSpec.Name))
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}
	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}

func NewRuntimeServiceServer(
	version string,
	jobSvc models.JobService,
	datastoreSvc models.DatastoreService,
	projectRepoFactory ProjectRepoFactory,
	namespaceRepoFactory NamespaceRepoFactory,
	secretRepoFactory SecretRepoFactory,
	adapter ProtoAdapter,
	progressObserver progress.Observer,
	instSvc models.InstanceService,
	scheduler models.SchedulerUnit,
) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		version:              version,
		jobSvc:               jobSvc,
		resourceSvc:          datastoreSvc,
		adapter:              adapter,
		projectRepoFactory:   projectRepoFactory,
		namespaceRepoFactory: namespaceRepoFactory,
		progressObserver:     progressObserver,
		instSvc:              instSvc,
		scheduler:            scheduler,
		secretRepoFactory:    secretRepoFactory,
	}
}

type jobSyncObserver struct {
	stream pb.RuntimeService_DeployJobSpecificationServer
	log    logrus.FieldLogger
}

func (obs *jobSyncObserver) Notify(e progress.Event) {
	switch evt := e.(type) {
	case *job.EventJobUpload:
		resp := &pb.DeployJobSpecificationResponse{
			Success: true,
			Ack:     true,
			JobName: evt.Job.Name,
		}
		if evt.Err != nil {
			resp.Success = false
			resp.Message = evt.Err.Error()
		}

		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send deploy spec ack for: %s", evt.Job.Name))
		}
	case *job.EventJobRemoteDelete:
		resp := &pb.DeployJobSpecificationResponse{
			JobName: evt.Name,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send delete notification for: %s", evt.Name))
		}
	case *job.EventJobSpecUnknownDependencyUsed:
		resp := &pb.DeployJobSpecificationResponse{
			JobName: evt.Job,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send unknown dependency notification for: %s", evt.Job))
		}
	}
}

type resourceObserver struct {
	stream pb.RuntimeService_DeployResourceSpecificationServer
	log    logrus.FieldLogger
}

func (obs *resourceObserver) Notify(e progress.Event) {
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
			obs.log.Error(errors.Wrapf(err, "failed to send deploy spec ack for: %s", evt.Spec.Name))
		}
	}
}

type jobCheckObserver struct {
	stream pb.RuntimeService_CheckJobSpecificationsServer
	log    logrus.FieldLogger
}

func (obs *jobCheckObserver) Notify(e progress.Event) {
	switch evt := e.(type) {
	case *job.EventJobCheckFailed:
		resp := &pb.CheckJobSpecificationsResponse{
			Success: false,
			Ack:     true,
			JobName: evt.Name,
			Message: evt.String(),
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send check ack for: %s", evt.Name))
		}
	case *job.EventJobCheckSuccess:
		resp := &pb.CheckJobSpecificationsResponse{
			Success: true,
			Ack:     true,
			JobName: evt.Name,
		}
		if err := obs.stream.Send(resp); err != nil {
			obs.log.Error(errors.Wrapf(err, "failed to send check ack for: %s", evt.Name))
		}
	}
}
