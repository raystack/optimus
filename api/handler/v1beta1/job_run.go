package v1beta1

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
)

type JobRunServiceServer struct {
	jobSvc              models.JobService
	pluginRepo          models.PluginRepository
	projectService      service.ProjectService
	namespaceService    service.NamespaceService
	secretService       service.SecretService
	runSvc              service.JobRunService
	jobRunInputCompiler compiler.JobRunInputCompiler
	monitoringService   service.MonitoringService
	scheduler           models.SchedulerUnit
	l                   log.Logger
	pb.UnimplementedJobRunServiceServer
}

func (sv *JobRunServiceServer) GetJobTask(ctx context.Context, req *pb.GetJobTaskRequest) (*pb.GetJobTaskResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
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
	taskDestination, taskDependencies, err := sv.jobSvc.GetTaskDependencies(ctx, namespaceSpec, jobSpec)
	if err != nil {
		if errors.Is(err, service.ErrDependencyModNotFound) {
			return &pb.GetJobTaskResponse{Task: jobTaskSpec}, nil
		}
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
func (sv *JobRunServiceServer) RegisterInstance(ctx context.Context, req *pb.RegisterInstanceRequest) (*pb.RegisterInstanceResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "not able to find project")
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

	secrets, err := sv.secretService.GetSecrets(ctx, namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to get secrets %s", err.Error(), jobRun)
	}

	jobRunInput, err := sv.jobRunInputCompiler.Compile(ctx,
		namespaceSpec, secrets, jobRun.Spec, jobRun.ScheduledAt,
		instance.Data, instance.Type, instance.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to compile instance of job %s", err.Error(), req.GetJobName())
	}

	jobProto := ToJobSpecificationProto(jobRun.Spec)

	instanceProto := ToInstanceProto(instance)

	return &pb.RegisterInstanceResponse{
		Project:   ToProjectProto(projSpec),
		Job:       jobProto,
		Instance:  instanceProto,
		Namespace: ToNamespaceProto(namespaceSpec),
		Context: &pb.JobRunInputResponse{
			Envs:    jobRunInput.ConfigMap,
			Secrets: jobRunInput.SecretsMap,
			Files:   jobRunInput.FileMap,
		},
	}, nil
}

// JobRunInput is used to fetch task/hook compiled configuration and assets.
func (sv *JobRunServiceServer) JobRunInput(ctx context.Context, req *pb.JobRunInputRequest) (*pb.JobRunInputResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "not able to find project")
	}

	jobSpec, namespaceSpec, err := sv.jobSvc.GetByNameForProject(ctx, req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: job %s not found", err.Error(), req.GetJobName())
	}

	instanceType, err := models.ToInstanceType(utils.FromEnumProto(req.InstanceType.String(), "TYPE"))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: instance type %s not found", err.Error(), req.InstanceType.String())
	}

	instanceName := req.GetInstanceName()

	scheduledAt := req.GetScheduledAt().AsTime()

	var jobFetchError error
	var jobRunSpec models.JobRunSpec
	if req.JobrunId == "" {
		jobRunSpec, jobFetchError = sv.monitoringService.GetJobRunByScheduledAt(ctx, namespaceSpec, jobSpec, scheduledAt)
		if jobFetchError != nil {
			sv.l.Error(status.Errorf(codes.Internal, "%s: failed to get JobRun by ScheduledAt for job %s", jobFetchError.Error(), jobSpec.Name).Error())
		}
	} else {
		jobRunID, jobFetchError := uuid.Parse(req.JobrunId)
		if jobFetchError != nil {
			sv.l.Error(status.Errorf(codes.InvalidArgument, "%s: failed to parse uuid of job %s", jobFetchError.Error(), req.JobrunId).Error())
		} else {
			jobRunSpec, jobFetchError = sv.monitoringService.GetJobRunByRunID(ctx, jobRunID, jobSpec)
			if jobFetchError != nil {
				sv.l.Error(status.Errorf(codes.Internal, "%s: failed to get JobRun by jobRunId::%s for job %s ", jobFetchError.Error(), req.JobrunId, jobSpec.Name).Error())
			}
		}
	}

	var jobRunSpecData []models.JobRunSpecData

	if jobFetchError != nil || len(jobRunSpec.Data) == 0 {
		// use scheduled time as execution time to avoid JobRunInput failures
		jobRunSpecData, err = getJobRunSpecData(scheduledAt, scheduledAt, jobSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: job has an invalid window Config, or wrong scheduleTime:%s %s:%s", err.Error(), scheduledAt.String(), jobSpec.Name, namespaceSpec.Name)
		}
	} else {
		jobRunSpecData = jobRunSpec.Data
	}

	secrets, err := sv.secretService.GetSecrets(ctx, namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to get secrets %s:%s", err.Error(), jobSpec.Name, namespaceSpec.Name)
	}

	jobRunInput, err := sv.jobRunInputCompiler.Compile(ctx,
		namespaceSpec, secrets, jobSpec, scheduledAt,
		jobRunSpecData, instanceType, instanceName)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to compile instance of job %s", err.Error(), jobSpec.Name)
	}

	return &pb.JobRunInputResponse{
		Envs:    jobRunInput.ConfigMap,
		Secrets: jobRunInput.SecretsMap,
		Files:   jobRunInput.FileMap,
	}, nil
}

func getJobRunSpecData(executedAt time.Time, scheduledAt time.Time, jobSpec models.JobSpec) ([]models.JobRunSpecData, error) {
	startTime, err := jobSpec.Task.Window.GetStartTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	endTime, err := jobSpec.Task.Window.GetEndTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	jobRunSpecData := []models.JobRunSpecData{
		{
			Name:  models.ConfigKeyExecutionTime,
			Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDstart,
			Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDend,
			Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDestination,
			Value: jobSpec.ResourceDestination,
			Type:  models.InstanceDataTypeEnv,
		},
	}
	return jobRunSpecData, nil
}

func (sv *JobRunServiceServer) JobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "not able to find project")
	}

	_, _, err = sv.jobSvc.GetByNameForProject(ctx, req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to find the job %s for project %s", err.Error(),
			req.GetJobName(), req.GetProjectName())
	}

	jobStatuses, err := sv.scheduler.GetJobStatus(ctx, projSpec, req.GetJobName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to fetch jobRun %s", err.Error(),
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

func (sv *JobRunServiceServer) JobRun(ctx context.Context, req *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "not able to find project")
	}
	jobSpec, _, err := sv.jobSvc.GetByNameForProject(ctx, req.GetJobName(), projSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to find the job %s for project %s", err.Error(),
			req.GetJobName(), req.GetProjectName())
	}
	query, err := buildJobQuery(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s\nfailed to build query %s", err.Error(),
			req.GetJobName())
	}
	jobRuns, err := sv.runSvc.GetJobRunList(ctx, projSpec, jobSpec, query)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s\nfailed to fetch job run %s", err.Error(),
			req.GetJobName())
	}
	if len(jobRuns) == 0 {
		return nil, status.Errorf(codes.NotFound, "%s\n job runs not found ",
			req.GetJobName())
	}
	var runs []*pb.JobRun
	for _, run := range jobRuns {
		ts := timestamppb.New(run.ScheduledAt)
		runs = append(runs, &pb.JobRun{
			State:       run.Status.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobRunResponse{
		JobRuns: runs,
	}, nil
}

func (*JobRunServiceServer) GetWindow(_ context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	if err := req.GetScheduledAt().CheckValid(); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse schedule time %s", err.Error(), req.GetScheduledAt())
	}

	window, err := models.NewWindow(int(req.Version), req.GetTruncateTo(), req.GetOffset(), req.GetSize())
	if err != nil {
		return nil, err
	}
	if err := window.Validate(); err != nil {
		return nil, err
	}

	startTime, err := window.GetStartTime(req.GetScheduledAt().AsTime())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("error getting start time: %s", err.Error()))
	}
	endTime, err := window.GetEndTime(req.GetScheduledAt().AsTime())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("error getting end time: %s", err.Error()))
	}

	return &pb.GetWindowResponse{
		Start: timestamppb.New(startTime),
		End:   timestamppb.New(endTime),
	}, nil
}

func (sv *JobRunServiceServer) RunJob(ctx context.Context, req *pb.RunJobRequest) (*pb.RunJobResponse, error) {
	// create job run in db
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	var jobSpecs []models.JobSpec
	for _, jobSpecProto := range req.Specifications {
		jobSpec, err := FromJobProto(jobSpecProto, sv.pluginRepo)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: cannot adapt job %s", err.Error(), jobSpecProto.GetName())
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}
	if jobDeploymentDetails, err := sv.jobSvc.Run(ctx, namespaceSpec, jobSpecs); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to queue job for execution %s , %s", err.Error(), req.ProjectName, jobDeploymentDetails.Failures)
	}

	return &pb.RunJobResponse{}, nil
}

func NewJobRunServiceServer(l log.Logger, jobSvc models.JobService, projectService service.ProjectService, namespaceService service.NamespaceService, secretService service.SecretService, pluginRepo models.PluginRepository, instSvc service.JobRunService, jobRunInputCompiler compiler.JobRunInputCompiler, monitoringService service.MonitoringService, scheduler models.SchedulerUnit) *JobRunServiceServer {
	return &JobRunServiceServer{
		l:                   l,
		jobSvc:              jobSvc,
		pluginRepo:          pluginRepo,
		runSvc:              instSvc,
		jobRunInputCompiler: jobRunInputCompiler,
		scheduler:           scheduler,
		monitoringService:   monitoringService,
		namespaceService:    namespaceService,
		projectService:      projectService,
		secretService:       secretService,
	}
}

func buildJobQuery(req *pb.JobRunRequest) (*models.JobQuery, error) {
	var query *models.JobQuery
	if req.GetStartDate().AsTime().Unix() == 0 && req.GetEndDate().AsTime().Unix() == 0 {
		query = &models.JobQuery{
			Name:        req.GetJobName(),
			OnlyLastRun: true,
		}
		return query, nil
	}
	if req.GetStartDate().AsTime().Unix() == 0 {
		return nil, errors.New("empty start date is given")
	}
	if req.GetEndDate().AsTime().Unix() == 0 {
		return nil, errors.New("empty end date is given")
	}
	query = &models.JobQuery{
		Name:      req.GetJobName(),
		StartDate: req.GetStartDate().AsTime(),
		EndDate:   req.GetEndDate().AsTime(),
		Filter:    req.GetFilter(),
	}
	return query, nil
}
