package v1beta1

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
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
	Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag bool, forceFlag bool) (affectedDownstream []job.FullName, err error)
	Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (jobSpec *job.Job, err error)
	GetTaskInfo(ctx context.Context, task *job.Task) (*job.Task, error)
	GetByFilter(ctx context.Context, filters ...filter.FilterOpt) (jobSpecs []*job.Job, err error)
	ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, jobNamesToSkip []job.Name, logWriter writer.LogWriter) error
	Refresh(ctx context.Context, projectName tenant.ProjectName, logWriter writer.LogWriter, filters ...filter.FilterOpt) error
	Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, logWriter writer.LogWriter) error

	GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger)
	GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error)
	GetDownstream(ctx context.Context, job *job.Job, localJob bool) ([]*dto.Downstream, error)
}

func (jh *JobHandler) AddJobSpecifications(ctx context.Context, jobSpecRequest *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	jobTenant, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	var jobSpecs []*job.Spec
	me := errors.NewMultiError("add specs errors")
	for _, jobProto := range jobSpecRequest.Specs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			errMsg := fmt.Sprintf("cannot adapt job specification %s: %s", jobProto.Name, err.Error())
			jh.l.Error(errMsg)
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
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
		msg = fmt.Sprintf("job %s has been forced deleted. these downstream will be affected: %s", jobName, affectedDownstream)
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

	var jobSpecs []*job.Spec
	me := errors.NewMultiError("update specs errors")
	for _, jobProto := range jobSpecRequest.Specs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			errMsg := fmt.Sprintf("cannot adapt job specification %s: %s", jobProto.Name, err.Error())
			jh.l.Error(errMsg)
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
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
	if err != nil {
		errorMsg := "failed to get job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	return &pb.GetJobSpecificationResponse{
		Spec: toJobProto(jobSpec),
	}, nil
}

func (jh *JobHandler) GetJobSpecifications(ctx context.Context, req *pb.GetJobSpecificationsRequest) (*pb.GetJobSpecificationsResponse, error) {
	jobSpecs, merr := jh.jobService.GetByFilter(ctx,
		filter.WithString(filter.ResourceDestination, req.GetResourceDestination()),
		filter.WithString(filter.ProjectName, req.GetProjectName()),
		filter.WithString(filter.JobName, req.GetJobName()),
	)

	jobSpecResponseProtos := []*pb.JobSpecificationResponse{}
	for _, jobSpec := range jobSpecs {
		jobSpecResponseProtos = append(jobSpecResponseProtos, &pb.JobSpecificationResponse{
			ProjectName:   jobSpec.Tenant().ProjectName().String(),
			NamespaceName: jobSpec.Tenant().NamespaceName().String(),
			Job:           toJobProto(jobSpec),
		})
	}

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

	return &pb.ListJobSpecificationResponse{
		Jobs: jobSpecificationProtos,
	}, merr
}

func (*JobHandler) GetWindow(_ context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	if err := req.GetScheduledAt().CheckValid(); err != nil {
		return nil, fmt.Errorf("%w: failed to parse schedule time %s", err, req.GetScheduledAt())
	}

	window, err := models.NewWindow(int(req.Version), req.GetTruncateTo(), req.GetOffset(), req.GetSize())
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

	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		responseWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] received %d job specs", request.GetNamespaceName(), len(request.GetJobs())))

		jobTenant, err := tenant.NewTenant(request.ProjectName, request.NamespaceName)
		if err != nil {
			errMsg := fmt.Sprintf("invalid replace all job specifications request for %s: %s", request.GetNamespaceName(), err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		var jobSpecs []*job.Spec
		var jobNamesToSkip []job.Name
		for _, jobProto := range request.Jobs {
			jobSpec, err := fromJobProto(jobProto)
			if err != nil {
				errMsg := fmt.Sprintf("[%s] cannot adapt job specification %s: %s", request.GetNamespaceName(), jobProto.Name, err.Error())
				jh.l.Error(errMsg)
				responseWriter.Write(writer.LogLevelError, errMsg)

				jobNameToSkip, err := job.NameFrom(jobProto.Name)
				if err == nil {
					jobNamesToSkip = append(jobNamesToSkip, jobNameToSkip)
				}

				errNamespaces = append(errNamespaces, request.NamespaceName)
				continue
			}
			jobSpecs = append(jobSpecs, jobSpec)
		}

		if err := jh.jobService.ReplaceAll(stream.Context(), jobTenant, jobSpecs, jobNamesToSkip, responseWriter); err != nil {
			errMsg := fmt.Sprintf("replace all job specifications failure for namespace %s: %s", request.NamespaceName, err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
		}
	}
	if len(errNamespaces) > 0 {
		namespacesWithError := strings.Join(errNamespaces, ", ")
		return fmt.Errorf("error when replacing job specifications: [%s]", namespacesWithError)
	}
	return nil
}

func (jh *JobHandler) RefreshJobs(request *pb.RefreshJobsRequest, stream pb.JobSpecificationService_RefreshJobsServer) error {
	responseWriter := writer.NewRefreshJobResponseWriter(stream)

	projectName, err := tenant.ProjectNameFrom(request.ProjectName)
	if err != nil {
		errMsg := fmt.Sprintf("unable to adapt project %s: %s", request.ProjectName, err.Error())
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}

	projectFilter := filter.WithString(filter.ProjectName, projectName.String())
	namespacesFilter := filter.WithStringArray(filter.NamespaceNames, request.NamespaceNames)
	jobNamesFilter := filter.WithStringArray(filter.JobNames, request.JobNames)

	if err = jh.jobService.Refresh(stream.Context(), projectName, responseWriter, projectFilter, namespacesFilter, jobNamesFilter); err != nil {
		errMsg := fmt.Sprintf("job refresh failed for project %s: %s", request.ProjectName, err.Error())
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}
	responseWriter.Write(writer.LogLevelInfo, "jobs refreshed successfully")
	return nil
}

func (jh *JobHandler) CheckJobSpecifications(req *pb.CheckJobSpecificationsRequest, stream pb.JobSpecificationService_CheckJobSpecificationsServer) error {
	responseWriter := writer.NewCheckJobSpecificationResponseWriter(stream)
	jobTenant, err := tenant.NewTenant(req.ProjectName, req.NamespaceName)
	if err != nil {
		return err
	}

	me := errors.NewMultiError("check / validate job spec errors")
	jobSpecs := []*job.Spec{}
	for _, js := range req.Jobs {
		jobSpec, err := fromJobProto(js)
		if err != nil {
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	if err := jh.jobService.Validate(stream.Context(), jobTenant, jobSpecs, responseWriter); err != nil {
		me.Append(err)
	}

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

	jobTask, err := jh.jobService.GetTaskInfo(ctx, jobResult.Spec().Task())
	if err != nil {
		return nil, err
	}

	jobTaskSpec := &pb.JobTask{
		Name:        jobTask.Info().Name,
		Description: jobTask.Info().Description,
		Image:       jobTask.Info().Image,
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

	internalUpstreamProto, externalUpstreamProto, unknownUpstreamProto := toUpstreamProtos(upstreams)
	downstreamProto := toDownstreamProtos(downstreams)

	var httpUpstreamProto []*pb.HttpDependency
	if subjectJob.Spec().Upstream() != nil {
		httpUpstreamProto = toHTTPUpstreamProtos(subjectJob.Spec().Upstream().HTTPUpstreams())
	}

	return &pb.JobInspectResponse{
		BasicInfo: toBasicInfoSectionProto(subjectJob, basicInfoLogger.Messages),
		Upstreams: &pb.JobInspectResponse_UpstreamSection{
			ExternalDependency:  externalUpstreamProto,
			InternalDependency:  internalUpstreamProto,
			HttpDependency:      httpUpstreamProto,
			UnknownDependencies: unknownUpstreamProto,
			Notice:              upstreamLogs.Messages,
		},
		Downstreams: &pb.JobInspectResponse_DownstreamSection{
			DownstreamJobs: downstreamProto,
			Notice:         downstreamLogs.Messages,
		},
	}, nil
}
