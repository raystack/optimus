package v1beta1

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobHandler struct {
	jobService JobService
	pb.UnimplementedJobSpecificationServiceServer
}

type JobService interface {
	Add(ctx context.Context, jobs []*dto.JobSpec) error
}

/*
create tenant
convert to domain object
	pass to service layer
	1. Check
		- Validate the specs
		- Check if any invalid query / dry run
		- Compilation check before saving to db? ----> in scheduling context
			- Let's try to validate in here, not through compilation dry run
	2. Identify the job destination
	3. Identify the job sources -> later we can parse the sql, get the detail from resource context
	4. Persist to DB -> we can have 1 repository, in transaction
		- Job table
		- Job destination table -> bigquery://project.dataset.table
		- Job source table
	5. Send deployment request to scheduling context
*/

func (jh *JobHandler) AddJobSpecifications(ctx context.Context, jobSpecRequest *pb.AddJobSpecificationsRequest) (*pb.AddJobSpecificationsResponse, error) {
	tnnt, err := tenant.NewTenant(jobSpecRequest.ProjectName, jobSpecRequest.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	var jobs []*dto.JobSpec
	for _, jobProto := range jobSpecRequest.Specs {
		jobEntity, err := fromJobProto(tnnt, jobProto)
		if err != nil {
			return nil, errors.GRPCErr(err, "failed to add job specifications")
		}
		jobs = append(jobs, jobEntity)
	}
	if err := jh.jobService.Add(ctx, jobs); err != nil {
		return nil, errors.GRPCErr(err, "failed to add job specifications")
	}

	return nil, status.Errorf(codes.Unimplemented, "method AddJobSpecifications not implemented")
}

//func (jh *JobHandler) DeployJobSpecification(_ pb.JobSpecificationService_DeployJobSpecificationServer) error {
//	return status.Errorf(codes.Unimplemented, "method DeployJobSpecification not implemented")
//}
//func (jh *JobHandler) CreateJobSpecification(ctx context.Context, _ *pb.CreateJobSpecificationRequest) (*pb.CreateJobSpecificationResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method CreateJobSpecification not implemented")
//}
//
//func (jh *JobHandler) GetJobSpecification(ctx context.Context, _ *pb.GetJobSpecificationRequest) (*pb.GetJobSpecificationResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method GetJobSpecification not implemented")
//}
//func (jh *JobHandler) GetJobSpecifications(ctx context.Context, _ *pb.GetJobSpecificationsRequest) (*pb.GetJobSpecificationsResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method GetJobSpecifications not implemented")
//}
//func (jh *JobHandler) DeleteJobSpecification(ctx context.Context, _ *pb.DeleteJobSpecificationRequest) (*pb.DeleteJobSpecificationResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method DeleteJobSpecification not implemented")
//}
//func (jh *JobHandler) ListJobSpecification(ctx context.Context, _ *pb.ListJobSpecificationRequest) (*pb.ListJobSpecificationResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method ListJobSpecification not implemented")
//}
//func (jh *JobHandler) CheckJobSpecification(ctx context.Context, _ *pb.CheckJobSpecificationRequest) (*pb.CheckJobSpecificationResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method CheckJobSpecification not implemented")
//}
//func (jh *JobHandler) CheckJobSpecifications(_ *pb.CheckJobSpecificationsRequest, _ pb.JobSpecificationService_CheckJobSpecificationsServer) error {
//	return status.Errorf(codes.Unimplemented, "method CheckJobSpecifications not implemented")
//}
//func (jh *JobHandler) RefreshJobs(*pb.RefreshJobsRequest, pb.JobSpecificationService_RefreshJobsServer) error {
//	return status.Errorf(codes.Unimplemented, "method RefreshJobs not implemented")
//}
//func (jh *JobHandler) GetDeployJobsStatus(ctx context.Context, _ *pb.GetDeployJobsStatusRequest) (*pb.GetDeployJobsStatusResponse, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method GetDeployJobsStatus not implemented")
//}
