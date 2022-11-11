package v1beta1

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"

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
	// TODO: We don't need to differentiate the error. utilize in-built multierror
	Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) (jobErrors error, err error)
}

func (jh *JobHandler) AddJobSpecifications(ctx context.Context, jobSpecRequest *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	jobTenant, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	var jobSpecs []*job.Spec
	//TODO: utilize multierror
	var jobErrors error
	for _, jobProto := range jobSpecRequest.Specs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			jobErrors = multierror.Append(jobErrors, err)
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	jobAddErrors, err := jh.jobService.Add(ctx, jobTenant, jobSpecs)
	if err != nil {
		return nil, err
	}
	if jobAddErrors != nil {
		jobErrors = multierror.Append(jobErrors, jobAddErrors)
	}

	responseLog := fmt.Sprintf("jobs are created and queued for deployment on project %s", jobSpecRequest.GetProjectName())
	if jobErrors != nil {
		responseLog = fmt.Sprintf("%s with error: %s", responseLog, jobErrors.Error())
	}

	// TODO: deprecate deployment ID field. is this api being used? if not we can deprecate deployment id, the api will be synchronous. if being used, we can still deprecate as it will be sync.
	return &pb.AddJobSpecificationsResponse{
		Log: responseLog,
	}, nil
}
