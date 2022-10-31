package v1betat1

import (
	"context"

	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobRunService interface {
}

type Scheduler interface {
}

type JobRunHandler struct {
	l       log.Logger
	service JobRunService

	pb.UnimplementedJobRunServiceServer
}

//func (JobRunHandler) GetJobTask(ctx context.Context, req *pb.GetJobTaskRequest) (*pb.GetJobTaskResponse, error) {
//	// GetJobTask Should be part of the job BC
//	return nil, nil
//}

func (JobRunHandler) JobRunInput(context.Context, *pb.JobRunInputRequest) (*pb.JobRunInputResponse, error) {
	// Use project_name to get job, then use the information in tenant
	return nil, nil
}

func (JobRunHandler) JobStatus(context.Context, *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	// Old api, replaced by JobRun, Remove
	return nil, nil
}

func (JobRunHandler) JobRun(context.Context, *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	// This should be using optimus to look in job run information for upstream check
	return nil, nil
}

func (JobRunHandler) GetWindow(context.Context, *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
	// Need to copy, paste
	return nil, nil
}

func (JobRunHandler) RunJob(context.Context, *pb.RunJobRequest) (*pb.RunJobResponse, error) {
	// Remove
	return nil, status.Errorf(codes.Unimplemented, "run job api is deprecated")
}
