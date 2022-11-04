package v1beta1

import (
	"context"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, job_run.JobName, job_run.RunConfig) (job_run.ExecutorInput, error)
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

func (h JobRunHandler) JobRunInput(ctx context.Context, req *pb.JobRunInputRequest) (*pb.JobRunInputResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	jobName, err := job_run.JobNameFrom(req.GetJobName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	executor, err := job_run.ExecutorFromEnum(req.InstanceName, req.InstanceType.String())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	err = req.ScheduledAt.CheckValid()
	if err != nil {
		return nil, errors.GRPCErr(errors.InvalidArgument(job_run.EntityJobRun, "invalid scheduled_at"), "unable to get job run input for "+req.GetJobName())
	}

	runConfig, err := job_run.RunConfigFrom(executor, req.ScheduledAt.AsTime(), req.JobrunId)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	input, err := h.service.JobRunInput(ctx, projectName, jobName, runConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	return &pb.JobRunInputResponse{
		Envs:    input.Configs,
		Files:   input.Files,
		Secrets: input.Secrets,
	}, nil
}

//func (JobRunHandler) JobStatus(context.Context, *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
//	// Old api, replaced by JobRun, Remove
//	return nil, nil
//}

func (JobRunHandler) JobRun(context.Context, *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	// This should be using optimus to look in job run information for upstream check
	return nil, nil
}

func (h JobRunHandler) UploadToScheduler(ctx context.Context) error {
	return nil
}

//func (JobRunHandler) GetWindow(context.Context, *pb.GetWindowRequest) (*pb.GetWindowResponse, error) {
//	// Need to copy, paste
//	return nil, nil
//}

//func (JobRunHandler) RunJob(context.Context, *pb.RunJobRequest) (*pb.RunJobResponse, error) {
//	// Remove
//	return nil, status.Errorf(codes.Unimplemented, "run job api is deprecated")
//}

func NewJobRunHandler(l log.Logger, service JobRunService) *JobRunHandler {
	return &JobRunHandler{
		l:       l,
		service: service,
	}
}
