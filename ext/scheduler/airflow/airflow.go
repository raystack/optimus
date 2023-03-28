package airflow

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/kushsharma/parallel"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

//go:embed __lib.py
var SharedLib []byte

const (
	EntityAirflow = "Airflow"

	dagStatusBatchURL = "api/v1/dags/~/dagRuns/list"
	dagRunClearURL    = "api/v1/dags/%s/clearTaskInstances"
	airflowDateFormat = "2006-01-02T15:04:05+00:00"

	schedulerHostKey = "SCHEDULER_HOST"

	baseLibFileName = "__lib.py"
	jobsDir         = "dags"
	jobsExtension   = ".py"

	concurrentTicketPerSec = 40
	concurrentLimit        = 600
)

type Bucket interface {
	WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
	// ReadAll(ctx context.Context, key string) ([]byte, error)
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) error
	Close() error
}

type BucketFactory interface {
	New(ctx context.Context, tenant tenant.Tenant) (Bucket, error)
}

type DagCompiler interface {
	Compile(job *scheduler.JobWithDetails) ([]byte, error)
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
	l         log.Logger
	bucketFac BucketFactory
	client    Client
	compiler  DagCompiler

	projectGetter ProjectGetter
	secretGetter  SecretGetter
}

func (s *Scheduler) DeployJobs(ctx context.Context, tenant tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	spanCtx, span := startChildSpan(ctx, "DeployJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, tenant)
	if err != nil {
		return errors.AddErrContext(err, EntityAirflow, "error in creating storage client instance")
	}
	defer bucket.Close()

	err = bucket.WriteAll(spanCtx, filepath.Join(jobsDir, baseLibFileName), SharedLib, nil)
	if err != nil {
		s.l.Error("failed to upload __lib.py file")
		return errors.AddErrContext(err, EntityAirflow, "error in writing __lib.py file")
	}
	multiError := errors.NewMultiError("ErrorsInDeployJobs")
	runner := parallel.NewRunner(parallel.WithTicket(concurrentTicketPerSec), parallel.WithLimit(concurrentLimit))
	for _, job := range jobs {
		runner.Add(func(currentJob *scheduler.JobWithDetails) func() (interface{}, error) {
			return func() (interface{}, error) {
				return nil, s.compileAndUpload(ctx, currentJob, bucket)
			}
		}(job))
	}

	for _, result := range runner.Run() {
		multiError.Append(result.Err)
	}
	return errors.MultiToError(multiError)
}

// TODO list jobs should not refer from the scheduler, rather should list from db and it has nothing to do with scheduler.
func (s *Scheduler) ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error) {
	spanCtx, span := startChildSpan(ctx, "ListJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, t)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	var jobNames []string
	// get all items under namespace directory
	it := bucket.List(&blob.ListOptions{
		Prefix: pathForJobDirectory(jobsDir, t.NamespaceName().String()),
	})
	for {
		obj, err := it.Next(spanCtx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if strings.HasSuffix(obj.Key, jobsExtension) {
			jobNames = append(jobNames, jobNameFromPath(obj.Key, jobsExtension))
		}
	}
	return jobNames, nil
}

func (s *Scheduler) DeleteJobs(ctx context.Context, t tenant.Tenant, jobNames []string) error {
	spanCtx, span := startChildSpan(ctx, "DeleteJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, t)
	if err != nil {
		return err
	}
	multiError := errors.NewMultiError("ErrorsInDeleteJobs")
	for _, jobName := range jobNames {
		if strings.TrimSpace(jobName) == "" {
			multiError.Append(errors.InvalidArgument(EntityAirflow, "job name cannot be an empty string"))
			continue
		}
		blobKey := pathFromJobName(jobsDir, t.NamespaceName().String(), jobName, jobsExtension)
		if err := bucket.Delete(spanCtx, blobKey); err != nil {
			// ignore missing files
			if gcerrors.Code(err) != gcerrors.NotFound {
				multiError.Append(err)
			}
		}
	}
	err = deleteDirectoryIfEmpty(ctx, t.NamespaceName().String(), bucket)
	if err != nil {
		if gcerrors.Code(err) != gcerrors.NotFound {
			multiError.Append(err)
		}
	}
	return errors.MultiToError(multiError)
}

// deleteDirectoryIfEmpty remove jobs Folder if it exists
func deleteDirectoryIfEmpty(ctx context.Context, nsDirectoryIdentifier string, bucket Bucket) error {
	spanCtx, span := startChildSpan(ctx, "deleteDirectoryIfEmpty")
	span.End()

	jobsDir := pathForJobDirectory(jobsDir, nsDirectoryIdentifier)

	it := bucket.List(&blob.ListOptions{
		Prefix: jobsDir,
	})
	_, err := it.Next(spanCtx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return bucket.Delete(ctx, jobsDir)
		}
	}
	return nil
}

func (s *Scheduler) compileAndUpload(ctx context.Context, job *scheduler.JobWithDetails, bucket Bucket) error {
	namespaceName := job.Job.Tenant.NamespaceName().String()
	blobKey := pathFromJobName(jobsDir, namespaceName, job.Name.String(), jobsExtension)

	compiledJob, err := s.compiler.Compile(job)
	if err != nil {
		s.l.Error(fmt.Sprintf("failed compilation %s:%s, err:%s", namespaceName, blobKey, err.Error()))
		return errors.AddErrContext(err, EntityAirflow, "job:"+job.Name.String())
	}
	if err := bucket.WriteAll(ctx, blobKey, compiledJob, nil); err != nil {
		s.l.Error(fmt.Sprintf("failed to upload %s:%s, err:%s", namespaceName, blobKey, err.Error()))
		return errors.AddErrContext(err, EntityAirflow, "job: "+job.Name.String())
	}
	return nil
}

func pathFromJobName(prefix, namespace, jobName, suffix string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return fmt.Sprintf("%s%s", path.Join(prefix, namespace, jobName), suffix)
}

func pathForJobDirectory(prefix, namespace string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return path.Join(prefix, namespace)
}

func jobNameFromPath(filePath, suffix string) string {
	jobFileName := path.Base(filePath)
	return strings.TrimSuffix(jobFileName, suffix)
}

func (s *Scheduler) GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
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

func getDagRunRequest(jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) DagRunRequest {
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

	auth, err := s.secretGetter.Get(ctx, tnnt.ProjectName(), tnnt.NamespaceName().String(), tenant.SecretSchedulerAuth)
	if err != nil {
		return SchedulerAuth{}, err
	}

	schdHost := strings.ReplaceAll(host, "http://", "")
	return SchedulerAuth{
		host:  schdHost,
		token: auth.Value(),
	}, nil
}

func (s *Scheduler) Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error {
	return s.ClearBatch(ctx, t, jobName, executionTime, executionTime)
}

func (s *Scheduler) ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error {
	spanCtx, span := startChildSpan(ctx, "Clear")
	defer span.End()

	data := []byte(fmt.Sprintf(`{"start_date": %q, "end_date": %q, "dry_run": false, "reset_dag_runs": true, "only_failed": false}`,
		startExecutionTime.UTC().Format(airflowDateFormat),
		endExecutionTime.UTC().Format(airflowDateFormat)))
	req := airflowRequest{
		URL:    dagRunClearURL,
		method: http.MethodPost,
		param:  jobName.String(),
		body:   data,
	}
	schdAuth, err := s.getSchedulerAuth(ctx, tnnt)
	if err != nil {
		return err
	}
	_, err = s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return fmt.Errorf("failure reason for clearing airflow dag runs: %w", err)
	}
	return nil
}

func NewScheduler(l log.Logger, bucketFac BucketFactory, client Client, compiler DagCompiler, projectGetter ProjectGetter, secretGetter SecretGetter) *Scheduler {
	return &Scheduler{
		l:             l,
		bucketFac:     bucketFac,
		compiler:      compiler,
		client:        client,
		projectGetter: projectGetter,
		secretGetter:  secretGetter,
	}
}
