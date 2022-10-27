package v1betat1

import (
	"context"

	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobRunService interface {
	GetJob(ctx context.Context, tnnt tenant.Tenant, jobName job_run.JobName) (job_run.Job, error)
}

type Scheduler interface {
}

type JobRunHandler struct {
	l       log.Logger
	service JobRunService

	pb.UnimplementedJobRunServiceServer
}

func (JobRunHandler) GetJobTask(ctx context.Context, req *pb.GetJobTaskRequest) (*pb.GetJobTaskResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to get job task for job  "+req.GetJobName())
	}

	return &pb.GetJobTaskResponse{}, nil
}

func (JobRunHandler) JobRunInput(context.Context, *pb.JobRunInputRequest) (*pb.JobRunInputResponse, error) {
	return nil, nil
}

func (JobRunHandler) JobStatus(context.Context, *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	return nil, nil
}

func (JobRunHandler) JobRun(context.Context, *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	return nil, nil
}

func (JobRunHandler) GetWindow(context.Context, *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	return nil, nil
}

func (JobRunHandler) RunJob(context.Context, *pb.RunJobRequest) (*pb.RunJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "run job api is deprecated")
}
