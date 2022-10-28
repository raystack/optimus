package v1beta1

import (
	"fmt"

	"github.com/google/uuid"
	"golang.org/x/net/context"

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
	Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.JobSpec) (deploymentID uuid.UUID, jobErr error, systemErr error)
	Validate(ctx context.Context, jobs []*job.JobSpec) ([]*job.JobSpec, error)
}

func (jh *JobHandler) AddJobSpecifications(ctx context.Context, jobSpecRequest *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	jobTenant, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	var jobs []*job.JobSpec
	for _, jobProto := range jobSpecRequest.Specs {
		jobEntity, err := fromJobProto(jobTenant, jobProto)
		if err != nil {
			return nil, errors.GRPCErr(err, "failed to add job specifications")
		}
		jobs = append(jobs, jobEntity)
	}

	deploymentID, jobErr, err := jh.jobService.Add(ctx, jobTenant, jobs)
	if err != nil {
		if jobErr != nil {
			return nil, errors.GRPCErr(err, fmt.Sprintf("failed to add job specifications: %s", jobErr.Error()))
		}
		return nil, errors.GRPCErr(err, "failed to add job specifications %s")
	}

	responseLog := fmt.Sprintf("jobs are created and queued for deployment on project %s", jobSpecRequest.GetProjectName())
	if jobErr != nil {
		responseLog = fmt.Sprintf("%s with error: %s", responseLog, jobErr.Error())
	}

	return &pb.AddJobSpecificationsResponse{
		Log:          responseLog,
		DeploymentId: deploymentID.String(),
	}, nil
}
