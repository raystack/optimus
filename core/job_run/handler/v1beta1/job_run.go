package v1beta1

import (
	"context"
	"encoding/json"

	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, job_run.JobName, job_run.RunConfig) (*job_run.ExecutorInput, error)
	UpdateJobState(context.Context, job_run.Event) error
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, criteria *job_run.JobRunsCriteria) ([]*job_run.JobRunStatus, error)
	UploadToScheduler(ctx context.Context, projectName tenant.ProjectName, namespaceName string) error
}

type Notifier interface {
	Push(ctx context.Context, event job_run.Event) error
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

// JobRun currently gets the job runs from scheduler based on the criteria
// TODO: later should collect the job runs from optimus
func (h JobRunHandler) JobRun(ctx context.Context, req *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	jobName, err := job_run.JobNameFrom(req.GetJobName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	criteria, err := buildCriteriaForJobRun(req)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	var jobRuns []*job_run.JobRunStatus
	jobRuns, err = h.service.GetJobRuns(ctx, projectName, jobName, criteria) // TODO: return not found if not runs found
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {

		}
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	var runs []*pb.JobRun
	for _, run := range jobRuns {
		ts := timestamppb.New(run.ScheduledAt)
		runs = append(runs, &pb.JobRun{
			State:       run.State.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobRunResponse{JobRuns: runs}, nil
}

func buildCriteriaForJobRun(req *pb.JobRunRequest) (*job_run.JobRunsCriteria, error) {
	if !req.GetStartDate().IsValid() && !req.GetEndDate().IsValid() {
		return &job_run.JobRunsCriteria{
			Name:        req.GetJobName(),
			OnlyLastRun: true,
		}, nil
	}
	if !req.GetStartDate().IsValid() {
		return nil, errors.InvalidArgument(job_run.EntityJobRun, "empty start date is given")
	}
	if !req.GetEndDate().IsValid() {
		return nil, errors.InvalidArgument(job_run.EntityJobRun, "empty end date is given")
	}
	return &job_run.JobRunsCriteria{
		Name:      req.GetJobName(),
		StartDate: req.GetStartDate().AsTime(),
		EndDate:   req.GetEndDate().AsTime(),
		Filter:    req.GetFilter(),
	}, nil
}

func (h JobRunHandler) UploadToScheduler(ctx context.Context, req *pb.UploadToSchedulerRequest) (*pb.UploadToSchedulerResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get project "+req.GetProjectName())
	}
	err = h.service.UploadToScheduler(ctx, projectName, req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to upload to scheduler for "+projectName.String())
	}
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
