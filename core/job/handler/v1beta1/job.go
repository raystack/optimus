package v1beta1

import (
	"context"
	"fmt"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobHandler struct {
	jobService JobService

	pb.UnimplementedJobSpecificationServiceServer
}

func NewJobHandler(jobService JobService) *JobHandler {
	return &JobHandler{jobService: jobService}
}

type JobService interface {
	Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]job.Name, error)
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

	addedJobNames, err := jh.jobService.Add(ctx, jobTenant, jobSpecs)
	me.Append(err)

	if len(addedJobNames) == 0 {
		return nil, errors.GRPCErr(errors.MultiToError(me), "failed to add job specifications")
	}

	responseLog := fmt.Sprintf("jobs %s are created", addedJobNames)
	if len(me.Errors) > 0 {
		responseLog = fmt.Sprintf("%s with error: %s", responseLog, errors.MultiToError(err))
	}

	return &pb.AddJobSpecificationsResponse{
		Log: responseLog,
	}, nil
}
