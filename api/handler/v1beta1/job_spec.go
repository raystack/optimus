package v1beta1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
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
	responseWriter := writer.NewDeployJobSpecificationResponseWriter(stream)

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err // immediate error returned (grpc error level)
		}

		jobSpecs := sv.convertProtoToJobSpec(req.GetJobs())

		// Deploying only the modified jobs
		deployID, err := sv.jobSvc.Deploy(stream.Context(), req.GetProjectName(), req.GetNamespaceName(), jobSpecs, responseWriter)
		if err != nil {
			errMsg := fmt.Sprintf("error while deploying namespace %s: %s", req.NamespaceName, err.Error())
			sv.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)

			// deployment per namespace failed
			responseWriter.SendDeploymentID("")
			continue
		}

		successMsg := fmt.Sprintf("deployment for namespace %s success", req.NamespaceName)
		sv.l.Info(successMsg)
		responseWriter.Write(writer.LogLevelInfo, successMsg)

		responseWriter.SendDeploymentID(deployID.UUID().String())
	}

	sv.l.Info("job deployment is successfully submitted")

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
		jobProto := ToJobSpecificationProto(jobSpec)

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

func (sv *JobSpecServiceServer) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, stream pb.JobSpecificationService_CheckJobSpecificationsServer) error {
	responseWriter := writer.NewCheckJobSpecificationResponseWriter(stream)
	namespaceSpec, err := sv.namespaceService.Get(stream.Context(), req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	var reqJobs []models.JobSpec
	for _, jobProto := range req.GetJobs() {
		j, err := FromJobProto(jobProto, sv.pluginRepo)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: failed to adapt job %s", err.Error(), jobProto.Name)
		}
		reqJobs = append(reqJobs, j)
	}

	if err = sv.jobSvc.Check(stream.Context(), namespaceSpec, reqJobs, responseWriter); err != nil {
		return status.Errorf(codes.Internal, "failed to compile jobs\n%s", err.Error())
	}

	return nil
}

func (sv *JobSpecServiceServer) JobInspect(ctx context.Context, req *pb.JobInspectRequest) (*pb.JobInspectResponse, error) {
	logWriter := &writer.BufferedLogger{}

	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	var jobSpec models.JobSpec

	if req.GetJobName() != "" {
		// get job spec from DB
		jobSpec, err = sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "cannot obtain jobSpec from optimus server: \n%s", err.Error())
		}
	} else {
		// jobSpec must be provided by client
		jobSpec, err = FromJobProto(req.GetSpec(), sv.pluginRepo)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "cannot deserialize job: \n%s", err.Error())
		}
	}
	jobSpec.NamespaceSpec = namespaceSpec

	jobDestination, jobSources, err := sv.jobSvc.GetJobSourceAndDestination(ctx, jobSpec)
	if err != nil {
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("Unable to determine job destination and sources: \n%s", err.Error()))
	}

	jobSpec.ResourceDestination = jobDestination.URN()
	sv.jobSvc.Check(ctx, namespaceSpec, []models.JobSpec{jobSpec}, logWriter)

	sv.hightlightJobWarnings(ctx, jobSpec, logWriter)

	return &pb.JobInspectResponse{
		Spec:        ToJobSpecificationProto(jobSpec),
		Sources:     jobSources,
		Destination: jobSpec.ResourceDestination,
		Log:         logWriter.Messages,
	}, nil
}

func (sv *JobSpecServiceServer) hightlightJobWarnings(ctx context.Context, jobSpec models.JobSpec, logWriter writer.LogWriter) {
	if jobSpec.Behavior.CatchUp {
		logWriter.Write(writer.LogLevelWarning, "Catchup is enabled")
	}
	if dupDestJobName, err := sv.jobSvc.IsJobDestinationDuplicate(ctx, jobSpec); err != nil {
		logWriter.Write(writer.LogLevelError, " could not perform duplicate job destination check err:"+err.Error())
	} else if dupDestJobName != "" {
		logWriter.Write(writer.LogLevelWarning, " already a job already exists with same Destination:"+jobSpec.ResourceDestination+" existing jobName:"+dupDestJobName)
	}
}

func (sv *JobSpecServiceServer) CreateJobSpecification(ctx context.Context, req *pb.CreateJobSpecificationRequest) (*pb.CreateJobSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	logWriter := writer.NewLogWriter(sv.l)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	reqJobSpec := req.GetSpec()
	jobSpec, err := FromJobProto(reqJobSpec, sv.pluginRepo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot deserialize job: \n%s", err.Error())
	}
	jobSpecs := []models.JobSpec{jobSpec}

	deployID, err := sv.jobSvc.CreateAndDeploy(ctx, namespaceSpec, jobSpecs, logWriter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "job addition failed for project %s, error:: %s", req.GetProjectName(), err.Error())
	}

	runtimeDeployJobSpecificationCounter.Inc()

	return &pb.CreateJobSpecificationResponse{
		Success: true,
		Message: fmt.Sprintf("job is created and queued for deployment on project %s, with Deployment ID : %s", req.GetProjectName(), deployID.UUID().String()),
	}, nil
}

func (sv *JobSpecServiceServer) AddJobSpecifications(ctx context.Context, req *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	logWriter := writer.NewLogWriter(sv.l)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	reqJobSpecs := req.GetSpecs()
	var jobSpecs []models.JobSpec
	for _, spec := range reqJobSpecs {
		jobSpec, err := FromJobProto(spec, sv.pluginRepo)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "cannot deserialize job: \n%s", err.Error())
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	deployID, err := sv.jobSvc.CreateAndDeploy(ctx, namespaceSpec, jobSpecs, logWriter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "jobs addition failed for project %s, error:: %s", req.GetProjectName(), err.Error())
	}

	runtimeDeployJobSpecificationCounter.Inc()

	return &pb.AddJobSpecificationsResponse{
		Log:          fmt.Sprintf("jobs are created and queued for deployment on project %s", req.GetProjectName()),
		DeploymentId: deployID.UUID().String(),
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

	jobSpecAdapt := ToJobSpecificationProto(jobSpec)

	return &pb.GetJobSpecificationResponse{
		Spec: jobSpecAdapt,
	}, nil
}

func (sv *JobSpecServiceServer) GetJobSpecifications(ctx context.Context, req *pb.GetJobSpecificationsRequest) (*pb.GetJobSpecificationsResponse, error) {
	jobSpecFilter := models.JobSpecFilter{
		ProjectName:         req.GetProjectName(),
		JobName:             req.GetJobName(),
		ResourceDestination: req.GetResourceDestination(),
	}
	jobSpecs, err := sv.jobSvc.GetByFilter(ctx, jobSpecFilter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve job: %s", err.Error())
	}
	jobSpecProtos := make([]*pb.JobSpecificationResponse, len(jobSpecs))
	for i, jobSpec := range jobSpecs {
		jobSpecProtos[i] = ToJobSpecificationResponseProto(jobSpec)
	}
	return &pb.GetJobSpecificationsResponse{JobSpecificationResponses: jobSpecProtos}, nil
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

// TODO RefreshJobs to save the loaded jobspecs to avoid writing oldWindowSize & oldWindowOffset temporarily
func (sv *JobSpecServiceServer) RefreshJobs(req *pb.RefreshJobsRequest, stream pb.JobSpecificationService_RefreshJobsServer) error {
	startTime := time.Now()
	responseWriter := writer.NewRefreshJobResponseWriter(stream)

	deployID, err := sv.jobSvc.Refresh(stream.Context(), req.ProjectName, req.NamespaceNames, req.JobNames, responseWriter)
	if err != nil {
		errMsg := "Unable to request job deployment"
		sv.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return status.Errorf(codes.Internal, "failed to refresh jobs: \n%s", err.Error())
	}

	sv.l.Info("finished job refresh", "time", time.Since(startTime))
	successMsg := fmt.Sprintf("Deployment request created with ID: %s", deployID.UUID().String())
	responseWriter.Write(writer.LogLevelInfo, successMsg)

	responseWriter.SendDeploymentID(deployID.UUID().String())
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
		unknownDependencies := make(map[string]string)
		for jobName, dependencies := range jobDeployment.Details.UnknownDependenciesPerJobName {
			unknownDependencies[jobName] = strings.Join(dependencies, ", ")
		}
		return &pb.GetDeployJobsStatusResponse{
			Status:              jobDeployment.Status.String(),
			SuccessCount:        int32(jobDeployment.Details.SuccessCount),
			FailureCount:        int32(len(jobDeployment.Details.Failures)),
			Failures:            deployJobFailures,
			UnknownDependencies: unknownDependencies,
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
