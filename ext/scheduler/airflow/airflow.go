package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
)

const (
	dagStatusBatchURL = "api/v1/dags/~/dagRuns/list"
	airflowDateFormat = "2006-01-02T15:04:05+00:00"

	schedulerHostKey = "SCHEDULER_HOST"
	schedulerAuthKey = "SCHEDULER_AUTH"
)

type Bucket interface {
	//WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
	//ReadAll(ctx context.Context, key string) ([]byte, error)
	//List(opts *blob.ListOptions) *blob.ListIterator
	//Delete(ctx context.Context, key string) error
	//Close() error
}

type BucketFactory interface {
	New(ctx context.Context, tenant tenant.Tenant) (Bucket, error)
}

type DagCompiler interface {
	Compile(job *job_run.Job) ([]byte, error)
}

type Client interface {
	Invoke(ctx context.Context, r airflowRequest, auth SchedulerAuth) ([]byte, error)
}

type SecretGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type Scheduler struct {
	bucketFac BucketFactory
	client    Client
	compiler  DagCompiler

	projectGetter ProjectGetter
	secretGetter  SecretGetter
}

func (s *Scheduler) GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *job_run.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*job_run.JobRunStatus, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRuns")
	defer span.End()

	dagRunRequest := getDagRunRequest(jobQuery, jobCron)
	reqBody, err := json.Marshal(dagRunRequest)
	if err != nil {
		return nil, err
	}

	req := airflowRequest{
		URL:    dagStatusBatchURL,
		method: http.MethodPost,
		body:   reqBody,
	}

	schdAuth, err := s.getSchedulerAuth(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return nil, fmt.Errorf("failure reason for fetching airflow dag runs: %w", err)
	}

	var dagRunList DagRunListResponse
	if err := json.Unmarshal(resp, &dagRunList); err != nil {
		return nil, fmt.Errorf("json error on parsing airflow dag runs: %s: %w", string(resp), err)
	}

	return getJobRuns(dagRunList, jobCron)
}

func getDagRunRequest(jobQuery *job_run.JobRunsCriteria, jobCron *cron.ScheduleSpec) DagRunRequest {
	if jobQuery.OnlyLastRun {
		return DagRunRequest{
			OrderBy:    "-execution_date",
			PageOffset: 0,
			PageLimit:  1,
			DagIds:     []string{jobQuery.Name},
		}
	}
	startDate := jobQuery.ExecutionStart(jobCron)
	endDate := jobQuery.ExecutionEndDate(jobCron)
	return DagRunRequest{
		OrderBy:          "execution_date",
		PageOffset:       0,
		PageLimit:        pageLimit,
		DagIds:           []string{jobQuery.Name},
		ExecutionDateGte: startDate.Format(airflowDateFormat),
		ExecutionDateLte: endDate.Format(airflowDateFormat),
	}
}

func (s *Scheduler) getSchedulerAuth(ctx context.Context, tnnt tenant.Tenant) (SchedulerAuth, error) {
	project, err := s.projectGetter.Get(ctx, tnnt.ProjectName())
	if err != nil {
		return SchedulerAuth{}, err
	}

	host, err := project.GetConfig(schedulerHostKey)
	if err != nil {
		return SchedulerAuth{}, err
	}

	auth, err := s.secretGetter.Get(ctx, tnnt.ProjectName(), tnnt.NamespaceName().String(), schedulerAuthKey)
	if err != nil {
		return SchedulerAuth{}, err
	}

	schdHost := strings.ReplaceAll(host, "http://", "")
	return SchedulerAuth{
		host:  schdHost,
		token: auth.Value(),
	}, nil
}

func NewScheduler(bucketFac BucketFactory, client Client, compiler DagCompiler, projectGetter ProjectGetter, secretGetter SecretGetter) *Scheduler {
	return &Scheduler{
		bucketFac:     bucketFac,
		compiler:      compiler,
		client:        client,
		projectGetter: projectGetter,
		secretGetter:  secretGetter,
	}
}
