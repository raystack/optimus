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
	GetAll(ctx context.Context, filters ...filter.FilterOpt) (jobSpecs []*job.Job, err error)
	ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, logWriter writer.LogWriter) error
	Refresh(ctx context.Context, projectName tenant.ProjectName, logWriter writer.LogWriter, filters ...filter.FilterOpt) error
	Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, logWriter writer.LogWriter) error
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
			errMsg := fmt.Sprintf("%s: cannot adapt job specification %s", err.Error(), jobProto.Name)
			jh.l.Error(errMsg)
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	if err = jh.jobService.Add(ctx, jobTenant, jobSpecs); err != nil {
		jh.l.Error(fmt.Sprintf("%s: failure found when adding job specifications", err.Error()))
		me.Append(err)
	}

	var responseLog string
	if len(me.Errors) > 0 {
		responseLog = fmt.Sprintf("adding jobs finished with error: %s", errors.MultiToError(err))
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
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	jobName, err := job.NameFrom(deleteRequest.JobName)
	if err != nil {
		errorMsg := "failed to adapt job name when deleting job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	affectedDownstream, err := jh.jobService.Delete(ctx, jobTenant, jobName, deleteRequest.CleanHistory, deleteRequest.Force)
	if err != nil {
		errorMsg := "failed to delete job specification"
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
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
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), errorMsg))
		return nil, errors.GRPCErr(err, errorMsg)
	}

	var jobSpecs []*job.Spec
	me := errors.NewMultiError("update specs errors")
	for _, jobProto := range jobSpecRequest.Specs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			errMsg := fmt.Sprintf("%s: cannot adapt job specification %s", err.Error(), jobProto.Name)
			jh.l.Error(errMsg)
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	if err = jh.jobService.Update(ctx, jobTenant, jobSpecs); err != nil {
		jh.l.Error(fmt.Sprintf("%s: %s", err.Error(), "failed to update job specifications"))
		me.Append(err)
	}

	var responseLog string
	if len(me.Errors) > 0 {
		responseLog = fmt.Sprintf("update jobs finished with error: %s", errors.MultiToError(err))
	} else {
		responseLog = "jobs are successfully updated"
	}

	return &pb.UpdateJobSpecificationsResponse{
		Log: responseLog,
	}, nil
}

func (jh *JobHandler) GetJobSpecification(ctx context.Context, req *pb.GetJobSpecificationRequest) (*pb.GetJobSpecificationResponse, error) {
	// TODO: need to have further analysis if this api is stil needed or not
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
	jobSpecs, merr := jh.jobService.GetAll(ctx,
		filter.WithString(filter.ResourceDestination, req.GetResourceDestination()),
		filter.WithString(filter.ProjectName, req.GetProjectName()),
		filter.WithString(filter.JobName, req.GetJobName()),
	)

	jobSpecResponseProtos := []*pb.JobSpecificationResponse{}
	for _, jobSpec := range jobSpecs {
		jobSpecResponseProtos = append(jobSpecResponseProtos, &pb.JobSpecificationResponse{
			// TODO: is it necessary to retrieve back the project name and namespace name?
			Job: toJobProto(jobSpec),
		})
	}

	return &pb.GetJobSpecificationsResponse{
		JobSpecificationResponses: jobSpecResponseProtos,
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

		jobTenant, err := tenant.NewTenant(request.ProjectName, request.NamespaceName)
		if err != nil {
			errMsg := fmt.Sprintf("invalid replace all job specifications request for %s: %s", request.GetNamespaceName(), err.Error())
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		var jobSpecs []*job.Spec
		for _, jobProto := range request.Jobs {
			jobSpec, err := fromJobProto(jobProto)
			if err != nil {
				errMsg := fmt.Sprintf("%s: cannot adapt job specification %s", err.Error(), jobProto.Name)
				jh.l.Error(errMsg)
				responseWriter.Write(writer.LogLevelError, errMsg)
				errNamespaces = append(errNamespaces, request.NamespaceName)
				continue
			}
			jobSpecs = append(jobSpecs, jobSpec)
		}

		if err := jh.jobService.ReplaceAll(stream.Context(), jobTenant, jobSpecs, responseWriter); err != nil {
			errMsg := fmt.Sprintf("%s: replace all job specifications failure for namespace %s", err.Error(), request.NamespaceName)
			jh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
		}
	}
	if len(errNamespaces) > 0 {
		namespacesWithError := strings.Join(errNamespaces, ", ")
		return fmt.Errorf("error when replacing job specifications: [%s]", namespacesWithError)
	}
	responseWriter.Write(writer.LogLevelInfo, "jobs replaced successfully")
	return nil
}

func (jh *JobHandler) RefreshJobs(request *pb.RefreshJobsRequest, stream pb.JobSpecificationService_RefreshJobsServer) error {
	responseWriter := writer.NewRefreshJobResponseWriter(stream)

	projectName, err := tenant.ProjectNameFrom(request.ProjectName)
	if err != nil {
		errMsg := fmt.Sprintf("%s: unable to adapt project %s", err.Error(), request.ProjectName)
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}

	projectFilter := filter.WithString(filter.ProjectName, projectName.String())
	namespacesFilter := filter.WithStringArray(filter.NamespaceNames, request.NamespaceNames)
	jobNamesFilter := filter.WithStringArray(filter.JobNames, request.JobNames)

	if err = jh.jobService.Refresh(stream.Context(), projectName, responseWriter, projectFilter, namespacesFilter, jobNamesFilter); err != nil {
		errMsg := fmt.Sprintf("%s: job refresh failed for project %s", err.Error(), request.ProjectName)
		jh.l.Error(errMsg)
		responseWriter.Write(writer.LogLevelError, errMsg)
		return err
	}
	responseWriter.Write(writer.LogLevelInfo, "jobs refreshed successfully")
	return nil
}

func (*JobHandler) CheckJobSpecification(ctx context.Context, req *pb.CheckJobSpecificationRequest) (*pb.CheckJobSpecificationResponse, error) {
	// TODO: need to do further investigation if this api is still being used or not
	return nil, nil
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
		return err
	}
	return me
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

	jobDetails, err := jh.jobService.Get(ctx, jobTenant, jobName)
	if err != nil {
		return nil, err
	}

	jobTask, err := jh.jobService.GetTaskInfo(ctx, jobDetails.Spec().Task())
	if err != nil {
		return nil, err
	}

	jobTaskSpec := &pb.JobTask{
		Name:        jobTask.Info().Name,
		Description: jobTask.Info().Description,
		Image:       jobTask.Info().Image,
	}

	jobTaskSpec.Destination = &pb.JobTask_Destination{
		Destination: jobDetails.Destination().String(),
		// TODO: investigate destination type
	}

	jobTaskSpec.Dependencies = make([]*pb.JobTask_Dependency, len(jobDetails.Sources()))
	for i, source := range jobDetails.Sources() {
		jobTaskSpec.Dependencies[i] = &pb.JobTask_Dependency{
			Dependency: source.String(),
		}
	}

	return &pb.GetJobTaskResponse{
		Task: jobTaskSpec,
	}, nil
}
