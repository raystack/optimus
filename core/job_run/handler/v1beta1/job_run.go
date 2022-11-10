package v1beta1

import (
	"context"
	"encoding/json"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, job_run.JobName, job_run.RunConfig) (*job_run.ExecutorInput, error)
	UpdateJobState(context.Context, job_run.Event) error
}

type Notifier interface {
	Push(context.Context, job_run.Event) error
}

type JobRunHandler struct {
	l        log.Logger
	service  JobRunService
	notifier Notifier

	pb.UnimplementedJobRunServiceServer
}

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

func (JobRunHandler) JobRun(context.Context, *pb.JobRunRequest) (*pb.JobRunResponse, error) {

	return nil, nil
}

func (h JobRunHandler) UploadToScheduler(ctx context.Context, req *pb.UploadToSchedulerRequest) (*pb.UploadToSchedulerResponse, error) {
	return nil, nil
}

func (h JobRunHandler) RegisterEvent(ctx context.Context, req *pb.RegisterJobEventRequest) (*pb.RegisterJobEventResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get tenant")
	}

	jobName, err := job_run.JobNameFrom(req.GetJobName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job name for "+req.GetJobName())
	}

	event, err := job_run.EventFrom(
		req.GetEvent().Type.String(),
		req.GetEvent().Value.AsMap(),
		jobName, tnnt,
	)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to parse event "+req.GetEvent().String())
	}

	err = h.service.UpdateJobState(ctx, event)
	if err != nil {
		jobEventByteString, _ := json.Marshal(event)
		h.l.Error("Scheduler event not registered, event Payload::", string(jobEventByteString), "error:", err.Error())
	}

	err = h.notifier.Push(ctx, event)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func NewJobRunHandler(l log.Logger, service JobRunService, notifier Notifier) *JobRunHandler {
	return &JobRunHandler{
		l:        l,
		service:  service,
		notifier: notifier,
	}
}
