package v1beta1

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/writer"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/sdk/plugin"
)

var (
	jobReplaceAllDurationGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jobs_replace_all_duration_in_seconds",
		Help: "The duration of job replace all in seconds",
	})
	jobRefreshDurationGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jobs_refresh_duration_in_seconds",
		Help: "The duration of job refresh in seconds",
	})
	jobValidationDurationGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jobs_validation_duration_in_seconds",
		Help: "The duration of job validation in seconds",
	})
)

type JobHandler struct {
	l          log.Logger
	jobService JobService

	pb.UnimplementedJobSpecificationServiceServer
}

func NewJobHandler(jobService JobService, logger log.Logger) *JobHandler {
	return &JobHandler{
		jobService: jobService,
		l:          logger,
	}
}

type JobService interface {
	Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error
	Update(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error
	Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag, forceFlag bool) (affectedDownstream []job.FullName, err error)
	Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (jobSpec *job.Job, err error)
	GetTaskInfo(ctx context.Context, task job.Task) (*plugin.Info, error)
	GetByFilter(ctx context.Context, filters ...filter.FilterOpt) (jobSpecs []*job.Job, err error)
	ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, jobNamesWithValidationError []job.Name, logWriter writer.LogWriter) error
	Refresh(ctx context.Context, projectName tenant.ProjectName, namespaceNames, jobNames []string, logWriter writer.LogWriter) error
	Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, logWriter writer.LogWriter) error

	GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger)
	GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error)
	GetDownstream(ctx context.Context, job *job.Job, localJob bool) ([]*job.Downstream, error)
}

func (jh *JobHandler) AddJobSpecifications(ctx context.Context, jobSpecRequest *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	jobTenant, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	me := errors.NewMultiError("add specs errors")

	jobSpecs, _, err := fromJobProtos(jobSpecRequest.Specs)
	if err != nil {
		errorMsg := fmt.Sprintf("failure when adapting job specifications: %s", err.Error())
		jh.l.Error(errorMsg)
		me.Append(err)
	}

	if len(jobSpecs) == 0 {
		me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, "no jobs to be processed"))
		return nil, errors.MultiToError(me)
	}

	if err = jh.jobService.Add(ctx, jobTenant, jobSpecs); err != nil {
		jh.l.Error(fmt.Sprintf("failure found when adding job specifications: %s", err.Error()))
		me.Append(err)
	}

	var responseLog string
	if len(me.Errors) > 0 {
		responseLog = fmt.Sprintf("adding jobs finished with error: %s", errors.MultiToError(me))
	} else {
		responseLog = "jobs are successfully created"
	}

	return &pb.AddJobSpecificationsResponse{
		Log: responseLog,
	}, nil
}

func (jh *JobHandler) DeleteJobSpecification(ctx context.Context, deleteRequest *pb.DeleteJobSpecificationRequest) (*pb.DeleteJobSpecificationResponse, error) {
	jobTenant, err := tenant.NewTenant(deleteRequest.ProjectName, deleteRequest.NamespaceName)
	if err != nil {
		errorMsg := "failed to adapt tenant when deleting job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", errorMsg, err.Error()))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	jobName, err := job.NameFrom(deleteRequest.JobName)
	if err != nil {
		errorMsg := "failed to adapt job name when deleting job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", errorMsg, err.Error()))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	affectedDownstream, err := jh.jobService.Delete(ctx, jobTenant, jobName, deleteRequest.CleanHistory, deleteRequest.Force)
	if err != nil {
		errorMsg := "failed to delete job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", errorMsg, err.Error()))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	msg := fmt.Sprintf("job %s has been deleted", jobName)
	if deleteRequest.Force && len(affectedDownstream) > 0 {
		msg = fmt.Sprintf("job %s has been forced deleted. these downstream will be affected: %s", jobName, job.FullNames(affectedDownstream).String())
	}

	return &pb.DeleteJobSpecificationResponse{
		Success: true,
		Message: msg,
	}, nil
}

func (jh *JobHandler) UpdateJobSpecifications(ctx context.Context, jobSpecRequest *pb.UpdateJobSpecificationsRequest) (*pb.UpdateJobSpecificationsResponse, error) {
	jobTenant, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		errorMsg := "failed to adapt tenant when updating job specifications"
		jh.l.Error(fmt.Sprintf("%s: %s", errorMsg, err.Error()))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	me := errors.NewMultiError("update specs errors")
	jobSpecs, _, err := fromJobProtos(jobSpecRequest.Specs)
	if err != nil {
		errorMsg := fmt.Sprintf("failure when adapting job specifications: %s", err.Error())
		jh.l.Error(errorMsg)
		me.Append(err)
	}

	if len(jobSpecs) == 0 {
		me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, "no jobs to be processed"))
		return nil, errors.MultiToError(me)
	}

	if err = jh.jobService.Update(ctx, jobTenant, jobSpecs); err != nil {
		jh.l.Error(fmt.Sprintf("%s: %s", "failed to update job specifications", err.Error()))
		me.Append(err)
	}

	var responseLog string
	if len(me.Errors) > 0 {
		responseLog = fmt.Sprintf("update jobs finished with error: %s", errors.MultiToError(me))
	} else {
		responseLog = "jobs are successfully updated"
	}

	return &pb.UpdateJobSpecificationsResponse{
		Log: responseLog,
	}, nil
}

func (jh *JobHandler) GetJobSpecification(ctx context.Context, req *pb.GetJobSpecificationRequest) (*pb.GetJobSpecificationResponse, error) {
	jobTenant, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, err
	}
	jobName, err := job.NameFrom(req.GetJobName())
	if err != nil {
		return nil, err
	}

	jobSpec, err := jh.jobService.Get(ctx, jobTenant, jobName)
	if err != nil && !errors.IsErrorType(err, errors.ErrNotFound) {
		errorMsg := "failed to get job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	// TODO: return 404 if job is not found
	return &pb.GetJobSpecificationResponse{
		Spec: toJobProto(jobSpec),
	}, nil
}

func (jh *JobHandler) GetJobSpecifications(ctx context.Context, req *pb.GetJobSpecificationsRequest) (*pb.GetJobSpecificationsResponse, error) {
	jobSpecs, merr := jh.jobService.GetByFilter(ctx,
		filter.WithString(filter.ResourceDestination, req.GetResourceDestination()),
		filter.WithString(filter.ProjectName, req.GetProjectName()),
		filter.WithString(filter.JobName, req.GetJobName()),
		filter.WithString(filter.NamespaceName, req.GetNamespaceName()),
	)

	jobSpecResponseProtos := []*pb.JobSpecificationResponse{}
	for _, jobSpec := range jobSpecs {
		jobSpecResponseProtos = append(jobSpecResponseProtos, &pb.JobSpecificationResponse{
			ProjectName:   jobSpec.Tenant().ProjectName().String(),
			NamespaceName: jobSpec.Tenant().NamespaceName().String(),
			Job:           toJobProto(jobSpec),
		})
	}

	// TODO: return 404 if job is not found
	return &pb.GetJobSpecificationsResponse{
		JobSpecificationResponses: jobSpecResponseProtos,
	}, merr
}

func (jh *JobHandler) ListJobSpecification(ctx context.Context, req *pb.ListJobSpecificationRequest) (*pb.ListJobSpecificationResponse, error) {
	jobSpecs, merr := jh.jobService.GetByFilter(ctx,
		filter.WithString(filter.ProjectName, req.GetProjectName()),
		filter.WithStringArray(filter.NamespaceNames, []string{req.GetNamespaceName()}),
	)

	jobSpecificationProtos := make([]*pb.JobSpecification, len(jobSpecs))
	for i, jobSpec := range jobSpecs {
		jobSpecificationProtos[i] = toJobProto(jobSpec)
	}

	// TODO: make a stream response
	return &pb.ListJobSpecificationResponse{
		Jobs: jobSpecificationProtos,
	}, merr
}

func (*JobHandler) GetWindow(_ context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	// TODO: the default version to be deprecated & made mandatory in future releases
	version := 2
	if err := req.GetScheduledAt().CheckValid(); err != nil {
		return nil, fmt.Errorf("%w: failed to parse schedule time %s", err, req.GetScheduledAt())
	}

	if req.Version != 0 {
		version = int(req.Version)
	}
	window, err := models.NewWindow(version, req.GetTruncateTo(), req.GetOffset(), req.GetSize())
	if err != nil {
		return nil, err
	}
	if err := window.Validate(); err != nil {
		return nil, err
	}

	me := errors.NewMultiError("get window errors")

	startTime, err := window.GetStartTime(req.GetScheduledAt().AsTime())
	me.Append(err)

	endTime, err := window.GetEndTime(req.GetScheduledAt().AsTime())
	me.Append(err)

	if len(me.Errors) > 0 {
		return nil, me
	}

	return &pb.GetWindowResponse{
		Start: timestamppb.New(startTime),
		End:   timestamppb.New(endTime),
	}, nil
}

func (jh *JobHandler) ReplaceAllJobSpecifications(stream pb.JobSpecificationService_ReplaceAllJobSpecificationsServer) error {
	responseWriter := writer.NewReplaceAllJobSpecificationsResponseWriter(stream)
	var errNamespaces []string
	var errMessages []string

	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		responseWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] received %d job specs", request.GetNamespaceName(), len(request.GetJobs())))
		jh.l.Info("replacing all job specifications for project [%s] namespace [%s]", request.GetProjectName(), request.GetNamespaceName())
		startTime := time.Now()

		jobTenant, err := tenant.NewTenant(request.ProjectName, request.NamespaceName)
		if err != nil {
			errMsg := fmt.Sprintf("[%s] invalid replace all job specifications request: %s", request.GetNamespaceName(), err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			errMessages = append(errMessages, errMsg)
			continue
		}

		jobSpecs, jobNamesWithValidationErrors, err := fromJobProtos(request.Jobs)
		if err != nil {
			errMsg := fmt.Sprintf("[%s] failed to adapt job specifications: %s", request.GetNamespaceName(), err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			errMessages = append(errMessages, errMsg)
		}

		if err := jh.jobService.ReplaceAll(stream.Context(), jobTenant, jobSpecs, jobNamesWithValidationErrors, responseWriter); err != nil {
			errMsg := fmt.Sprintf("[%s] replace all job specifications failure: %s", request.NamespaceName, err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			errMessages = append(errMessages, errMsg)
		}

		processDuration := time.Since(startTime)
		jh.l.Info("finished replacing all job specifications for project [%s] namespace [%s], took %s", request.GetProjectName(), request.GetNamespaceName(), processDuration)
		jobReplaceAllDurationGauge.Set(processDuration.Seconds())
	}
	if len(errNamespaces) > 0 {
		errMessageSummary := strings.Join(errMessages, "\n")
		responseWriter.Write(writer.LogLevelError, fmt.Sprintf("\njob replace all finished with errors:\n%s", errMessageSummary))

		namespacesWithError := strings.Join(errNamespaces, ", ")
		return fmt.Errorf("error when replacing job specifications: [%s]", namespacesWithError)
	}
	return nil
}

func (jh *JobHandler) RefreshJobs(request *pb.RefreshJobsRequest, stream pb.JobSpecificationService_RefreshJobsServer) error {
	jh.l.Info("refreshing jobs for project [%s]", request.GetProjectName())
	startTime := time.Now()
	defer func() {
		processDuration := time.Since(startTime)
		jobRefreshDurationGauge.Set(processDuration.Seconds())
		jh.l.Info("finished refreshing jobs for project [%s], took %s", request.GetProjectName(), processDuration)
	}()

	responseWriter := writer.NewRefreshJobResponseWriter(stream)

	projectName, err := tenant.ProjectNameFrom(request.ProjectName)
	if err != nil {
		errMsg := fmt.Sprintf("unable to adapt project %s: %s", request.ProjectName, err.Error())
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}

	if err = jh.jobService.Refresh(stream.Context(), projectName, request.NamespaceNames, request.JobNames, responseWriter); err != nil {
		errMsg := fmt.Sprintf("job refresh failed for project %s: %s", request.ProjectName, err.Error())
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}
	responseWriter.Write(writer.LogLevelInfo, "jobs refreshed successfully")
	return nil
}

func (jh *JobHandler) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, stream pb.JobSpecificationService_CheckJobSpecificationsServer) error {
	jh.l.Info("validating jobs for project [%s] namespace [%s]", req.GetProjectName(), req.GetNamespaceName())
	startTime := time.Now()

	responseWriter := writer.NewCheckJobSpecificationResponseWriter(stream)
	jobTenant, err := tenant.NewTenant(req.ProjectName, req.NamespaceName)
	if err != nil {
		return err
	}

	me := errors.NewMultiError("check / validate job spec errors")
	jobSpecs, _, err := fromJobProtos(req.Jobs)
	if err != nil {
		errorMsg := fmt.Sprintf("failure when adapting job specifications: %s", err.Error())
		jh.l.Error(errorMsg)
		me.Append(err)
	}

	if err := jh.jobService.Validate(stream.Context(), jobTenant, jobSpecs, responseWriter); err != nil {
		me.Append(err)
	}

	processDuration := time.Since(startTime)
	jobValidationDurationGauge.Set(processDuration.Seconds())

	jh.l.Info("finished validating jobs for project [%s] namespace [%s], took %s", req.GetProjectName(), req.GetNamespaceName(), processDuration)

	return errors.MultiToError(me)
}

func (jh *JobHandler) GetJobTask(ctx context.Context, req *pb.GetJobTaskRequest) (*pb.GetJobTaskResponse, error) {
	jobTenant, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, err
	}

	jobName, err := job.NameFrom(req.GetJobName())
	if err != nil {
		return nil, err
	}

	jobResult, err := jh.jobService.Get(ctx, jobTenant, jobName)
	if err != nil {
		return nil, err
	}

	taskInfo, err := jh.jobService.GetTaskInfo(ctx, jobResult.Spec().Task())
	if err != nil {
		return nil, err
	}

	jobTaskSpec := &pb.JobTask{
		Name:        taskInfo.Name,
		Description: taskInfo.Description,
		Image:       taskInfo.Image,
	}

	jobTaskSpec.Destination = &pb.JobTask_Destination{
		Destination: jobResult.Destination().String(),
		// TODO: investigate destination type
	}

	jobTaskSpec.Dependencies = make([]*pb.JobTask_Dependency, len(jobResult.Sources()))
	for i, source := range jobResult.Sources() {
		jobTaskSpec.Dependencies[i] = &pb.JobTask_Dependency{
			Dependency: source.String(),
		}
	}

	return &pb.GetJobTaskResponse{
		Task: jobTaskSpec,
	}, nil
}

func (jh *JobHandler) JobInspect(ctx context.Context, req *pb.JobInspectRequest) (*pb.JobInspectResponse, error) {
	jobTenant, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, err
	}

	localJob := false
	var jobName job.Name
	var jobSpec *job.Spec
	if req.GetSpec() != nil {
		jobSpec, err = fromJobProto(req.GetSpec())
		if err != nil {
			errMsg := fmt.Sprintf("cannot adapt job specification %s: %s", req.Spec.Name, err.Error())
			jh.l.Error(errMsg)
			return nil, err
		}
		localJob = true
	} else {
		jobName, err = job.NameFrom(req.JobName)
		if err != nil {
			return nil, err
		}
	}

	subjectJob, basicInfoLogger := jh.jobService.GetJobBasicInfo(ctx, jobTenant, jobName, jobSpec)

	if subjectJob == nil && req.GetSpec() == nil {
		var basicInfoMsg []string
		for _, message := range basicInfoLogger.Messages {
			basicInfoMsg = append(basicInfoMsg, message.GetMessage())
		}
		return nil, errors.GRPCErr(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, strings.Join(basicInfoMsg, "\n")), "failed to inspect job")
	}

	upstreamLogs := &writer.BufferedLogger{}
	upstreams, err := jh.jobService.GetUpstreamsToInspect(ctx, subjectJob, localJob)
	if err != nil {
		upstreamLogs.Write(writer.LogLevelError, fmt.Sprintf("unable to get upstream jobs: %v", err.Error()))
	}

	downstreamLogs := &writer.BufferedLogger{}
	downstreams, err := jh.jobService.GetDownstream(ctx, subjectJob, localJob)
	if err != nil {
		downstreamLogs.Write(writer.LogLevelError, fmt.Sprintf("unable to get downstream jobs: %v", err.Error()))
	}

	upstreamProto := toUpstreamProtos(upstreams, subjectJob.Spec().UpstreamSpec(), upstreamLogs.Messages)
	downstreamProto := toDownstreamProtos(downstreams)

	return &pb.JobInspectResponse{
		BasicInfo: toBasicInfoSectionProto(subjectJob, basicInfoLogger.Messages),
		Upstreams: upstreamProto,
		Downstreams: &pb.JobInspectResponse_DownstreamSection{
			DownstreamJobs: downstreamProto,
			Notice:         downstreamLogs.Messages,
		},
	}, nil
}
