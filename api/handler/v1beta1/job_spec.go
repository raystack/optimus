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
	"google.golang.org/protobuf/types/known/timestamppb"

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
	jobRunService    service.JobRunService
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

func (sv *JobSpecServiceServer) getDpendencyRunInfo(ctx context.Context, jobSpec models.JobSpec, scheduleTime time.Time, logWriter writer.LogWriter) ([]*pb.JobInspectResponse_JobDependency, []*pb.JobInspectResponse_JobDependency, []*pb.HttpDependency) {
	var internalDependencies []*pb.JobInspectResponse_JobDependency
	var externalDependencies []*pb.JobInspectResponse_JobDependency
	var httpDependency []*pb.HttpDependency

	windowStartTime, err := jobSpec.Task.Window.GetStartTime(scheduleTime)
	if err != nil {
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("unable to get Window start time for %s/%s", jobSpec.GetProjectSpec().Name, jobSpec.Name))
		return internalDependencies, externalDependencies, httpDependency
	}
	windowEndTime, err := jobSpec.Task.Window.GetEndTime(scheduleTime)
	if err != nil {
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("unable to get Window end time for %s/%s", jobSpec.GetProjectSpec().Name, jobSpec.Name))
		return internalDependencies, externalDependencies, httpDependency
	}

	for dependencyName, dependency := range jobSpec.Dependencies {
		if dependency.Job == nil {
			delete(jobSpec.Dependencies, dependencyName)
			continue
		}

		dependencyProjectSpec, err := sv.projectService.Get(ctx, dependency.Job.GetProjectSpec().Name)
		if err != nil {
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("error in fetching project Spec for %s, err::%s", dependency.Job.GetFullName(), err.Error()))
			continue
		}
		jobRunList, err := sv.jobRunService.GetJobRunList(ctx, dependencyProjectSpec, *dependency.Job, &models.JobQuery{
			Name:      dependencyName,
			StartDate: windowStartTime,
			EndDate:   windowEndTime,
		})
		if err != nil {
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("error in fetching job run list for %s, err::%s", dependency.Job.GetFullName(), err.Error()))
			continue
		}
		var runsProto []*pb.JobRun
		for _, run := range jobRunList {
			runsProto = append(runsProto, &pb.JobRun{
				State:       run.Status.String(),
				ScheduledAt: timestamppb.New(run.ScheduledAt),
			})
		}
		internalDependencies = append(internalDependencies, &pb.JobInspectResponse_JobDependency{
			Name:          dependency.Job.Name,
			Host:          "internal",
			ProjectName:   dependency.Job.GetProjectSpec().Name,
			NamespaceName: dependency.Job.NamespaceSpec.Name,
			TaskName:      dependency.Job.Task.Unit.Info().Name,
			Runs:          runsProto,
		})
	}

	for _, dependency := range jobSpec.ExternalDependencies.OptimusDependencies {
		jobRunList, err := sv.jobSvc.GetExternalJobRuns(ctx, dependency.Host, dependency.JobName, dependency.ProjectName, windowStartTime, windowEndTime)
		if err != nil {
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("error in fetching job run list for External job %s::%s/%s, err::%s", dependency.Host, dependency.ProjectName, dependency.JobName, err.Error()))
		}
		var runsProto []*pb.JobRun
		for _, run := range jobRunList {
			runsProto = append(runsProto, &pb.JobRun{
				State:       run.Status.String(),
				ScheduledAt: timestamppb.New(run.ScheduledAt),
			})
		}
		externalDependencies = append(externalDependencies, &pb.JobInspectResponse_JobDependency{
			Name:          dependency.JobName,
			Host:          dependency.Host,
			ProjectName:   dependency.ProjectName,
			NamespaceName: dependency.NamespaceName,
			TaskName:      dependency.TaskName,
			Runs:          runsProto,
		})
	}
	for _, dependency := range jobSpec.ExternalDependencies.HTTPDependencies {
		httpDependency = append(httpDependency, &pb.HttpDependency{
			Name:    dependency.Name,
			Url:     dependency.URL,
			Params:  dependency.RequestParams,
			Headers: dependency.Headers,
		})
	}
	return internalDependencies, externalDependencies, httpDependency
}

func (sv *JobSpecServiceServer) JobInspect(ctx context.Context, req *pb.JobInspectRequest) (*pb.JobInspectResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}
	scheduleTime := req.GetScheduledAt().AsTime()
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

	jobBasicInfo := sv.jobSvc.GetJobBasicInfo(ctx, jobSpec)

	upstreamLogs := &writer.BufferedLogger{}
	if scheduleTime.Unix() == time.Unix(0, 0).Unix() {
		scheduleTime = time.Now()
		upstreamLogs.Write(writer.LogLevelInfo, fmt.Sprintf("schedule time not provided, using schedule time as current time::%v", scheduleTime))
	}
	jobSpec, unknownDependency, err := sv.jobSvc.GetEnrichedUpstreamJobSpec(ctx, jobSpec, jobBasicInfo.JobSource, upstreamLogs)
	if err != nil {
		upstreamLogs.Write(writer.LogLevelError, fmt.Sprintf("error while enriching upstreams dependeincies %v", err.Error()))
	}
	var unknownDependencyProto []*pb.JobInspectResponse_UpstreamSection_UnknownDependencies
	for _, dependency := range unknownDependency {
		unknownDependencyProto = append(unknownDependencyProto, &pb.JobInspectResponse_UpstreamSection_UnknownDependencies{
			JobName:     dependency.DependencyJobName,
			ProjectName: dependency.DependencyProjectName,
		})
	}

	internalDepsProto, externalDepsProto, httpDepsProto := sv.getDpendencyRunInfo(ctx, jobSpec, scheduleTime, upstreamLogs)

	downstreamLogs := &writer.BufferedLogger{}

	downStreamJobs, err := sv.jobSvc.GetDownstreamJobs(ctx, jobSpec.Name, jobSpec.ResourceDestination, jobSpec.GetProjectSpec().Name)
	if err != nil {
		downstreamLogs.Write(writer.LogLevelError, fmt.Sprintf("unable to get downstream jobs %v", err.Error()))
	}
	var downStreamJobsProtoSpecArray []*pb.JobInspectResponse_JobDependency
	for _, job := range downStreamJobs {
		downStreamJobsProtoSpecArray = append(downStreamJobsProtoSpecArray, &pb.JobInspectResponse_JobDependency{
			Name:          job.Name,
			ProjectName:   job.GetProjectSpec().Name,
			NamespaceName: job.NamespaceSpec.Name,
			TaskName:      job.Task.Unit.Info().Name,
		})
	}

	return &pb.JobInspectResponse{
		BasicInfo: ToBasicInfoSectionProto(jobBasicInfo),
		Upstreams: &pb.JobInspectResponse_UpstreamSection{
			ExternalDependency:  externalDepsProto,
			InternalDependency:  internalDepsProto,
			HttpDependency:      httpDepsProto,
			UnknownDependencies: unknownDependencyProto,
			Notice:              upstreamLogs.Messages,
		},
		Downstreams: &pb.JobInspectResponse_DownstreamSection{
			DownstreamJobs: downStreamJobsProtoSpecArray,
			Notice:         downstreamLogs.Messages,
		},
	}, nil
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

func NewJobSpecServiceServer(l log.Logger, jobService models.JobService, jobRunService service.JobRunService, pluginRepo models.PluginRepository,
	projectService service.ProjectService, namespaceService service.NamespaceService, progressObserver progress.Observer) *JobSpecServiceServer {
	return &JobSpecServiceServer{
		l:                l,
		jobSvc:           jobService,
		jobRunService:    jobRunService,
		pluginRepo:       pluginRepo,
		projectService:   projectService,
		namespaceService: namespaceService,
		progressObserver: progressObserver,
	}
}
