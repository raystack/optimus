package v1beta1

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type JobHandler struct {
	jobService JobService

	pb.UnimplementedJobSpecificationServiceServer
}

func NewJobHandler(jobService JobService) *JobHandler {
	return &JobHandler{jobService: jobService}
}

type JobService interface {
	Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error
	Update(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error
	Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag bool, forceFlag bool) (affectedDownstream []job.FullName, err error)
	Get(ctx context.Context, filters ...filter.FilterOpt) (jobSpec *job.Spec, err error)
	GetAll(ctx context.Context, filters ...filter.FilterOpt) (jobSpecs []*job.Spec, err error)
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
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	err = jh.jobService.Add(ctx, jobTenant, jobSpecs)
	me.Append(err)

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
		return nil, errors.GRPCErr(err, "failed to delete job specification")
	}

	jobName, err := job.NameFrom(deleteRequest.JobName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to delete job specification")
	}

	affectedDownstream, err := jh.jobService.Delete(ctx, jobTenant, jobName, deleteRequest.CleanHistory, deleteRequest.Force)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to delete job specification")
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
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	var jobSpecs []*job.Spec
	me := errors.NewMultiError("update specs errors")
	for _, jobProto := range jobSpecRequest.Specs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			me.Append(err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	err = jh.jobService.Update(ctx, jobTenant, jobSpecs)
	me.Append(err)

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
	jobSpec, err := jh.jobService.Get(ctx,
		filter.With(filter.ProjectName, req.GetProjectName()),
		filter.With(filter.JobName, req.GetJobName()),
	)
	if err != nil {
		return nil, err
	}

	return &pb.GetJobSpecificationResponse{
		Spec: toJobProto(jobSpec),
	}, nil
}

func (jh *JobHandler) GetJobSpecifications(ctx context.Context, req *pb.GetJobSpecificationsRequest) (*pb.GetJobSpecificationsResponse, error) {
	jobSpecs, merr := jh.jobService.GetAll(ctx,
		filter.With(filter.ResourceDestination, req.GetResourceDestination()),
		filter.With(filter.ProjectName, req.GetProjectName()),
		filter.With(filter.JobName, req.GetJobName()),
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

func (jh *JobHandler) GetWindow(_ context.Context, req *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	if err := req.GetScheduledAt().CheckValid(); err != nil {
		return nil, fmt.Errorf("%s: failed to parse schedule time %s", err.Error(), req.GetScheduledAt())
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
