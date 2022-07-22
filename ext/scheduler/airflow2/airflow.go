package airflow2

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
)

//go:embed resources/__lib.py
var SharedLib []byte

//go:embed resources/base_dag.py
var resBaseDAG []byte

var ErrEmptyJobName = errors.New("job name cannot be an empty string")

const (
	baseLibFileName   = "__lib.py"
	dagStatusURL      = "api/v1/dags/%s/dagRuns"
	dagStatusBatchURL = "api/v1/dags/~/dagRuns/list"
	dagRunClearURL    = "api/v1/dags/%s/clearTaskInstances"
	airflowDateFormat = "2006-01-02T15:04:05+00:00"

	JobsDir       = "dags"
	JobsExtension = ".py"

	ConcurrentTicketPerSec = 40
	ConcurrentLimit        = 600
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type BucketFactory interface {
	New(ctx context.Context, project models.ProjectSpec) (Bucket, error)
}

type Bucket interface {
	WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
	ReadAll(ctx context.Context, key string) ([]byte, error)
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) error
	Close() error
}

type scheduler struct {
	bucketFac BucketFactory
	client    airflowClient
	compiler  models.JobCompiler
}

func (*scheduler) GetName() string {
	return "airflow2"
}

func (*scheduler) GetTemplate() []byte {
	return resBaseDAG
}

func (s *scheduler) VerifyJob(_ context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
	_, err := s.compiler.Compile(s.GetTemplate(), namespace, job)
	return err
}

// DeployJobs is used by Deploy process
// Any progress is being sent through observer
// This will be deprecated when job deployment being done asynchronously
func (s *scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec,
	progressObserver progress.Observer,
) error {
	spanCtx, span := startChildSpan(ctx, "DeployJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, namespace.ProjectSpec)
	if err != nil {
		return err
	}
	defer bucket.Close()

	bucket.WriteAll(spanCtx, filepath.Join(JobsDir, baseLibFileName), SharedLib, nil)

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, j := range jobs {
		runner.Add(func(currentJobSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				compiledJob, err := s.compiler.Compile(s.GetTemplate(), namespace, currentJobSpec)
				if err != nil {
					return nil, err
				}
				s.notifyProgress(progressObserver, &models.ProgressJobSpecCompiled{
					Name: compiledJob.Name,
				})

				blobKey := PathFromJobName(JobsDir, namespace.ID.String(), compiledJob.Name, JobsExtension)
				if err := bucket.WriteAll(ctx, blobKey, compiledJob.Contents, nil); err != nil {
					s.notifyProgress(progressObserver, &models.ProgressJobUpload{
						Name: compiledJob.Name,
						Err:  err,
					})
					return nil, err
				}
				s.notifyProgress(progressObserver, &models.ProgressJobUpload{
					Name: compiledJob.Name,
					Err:  nil,
				})
				return nil, nil
			}
		}(j))
	}
	for _, result := range runner.Run() {
		if result.Err != nil {
			err = multierror.Append(err, result.Err)
		}
	}
	return err
}

// DeployJobsVerbose does not use observer but instead return the deployment detail
// This is being used by refresh command to deploy jobs after dependencies refreshed
// TODO: Deprecate the other DeployJobs and rename this.
func (s *scheduler) DeployJobsVerbose(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec) (models.JobDeploymentDetail, error) {
	spanCtx, span := startChildSpan(ctx, "DeployJobsVerbose")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, namespace.ProjectSpec)
	if err != nil {
		return models.JobDeploymentDetail{}, err
	}
	defer bucket.Close()

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, j := range jobs {
		runner.Add(func(currentJobSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				return s.compileAndUpload(ctx, namespace, currentJobSpec, bucket), nil
			}
		}(j))
	}

	var jobDeploymentDetail models.JobDeploymentDetail
	for _, result := range runner.Run() {
		if result.Val != nil {
			jobDeploymentDetail.Failures = append(jobDeploymentDetail.Failures, result.Val.(models.JobDeploymentFailure))
		}
	}

	jobDeploymentDetail.FailureCount = len(jobDeploymentDetail.Failures)
	jobDeploymentDetail.SuccessCount = len(jobs) - jobDeploymentDetail.FailureCount
	return jobDeploymentDetail, nil
}

// deleteDirectoryIfEmpty remove jobs Folder if it exists
func deleteDirectoryIfEmpty(ctx context.Context, jobsDir string, bucket Bucket) error {
	spanCtx, span := startChildSpan(ctx, "deleteDirectoryIfEmpty")
	span.End()

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

func (s *scheduler) compileAndUpload(ctx context.Context, namespace models.NamespaceSpec, currentJobSpec models.JobSpec, bucket Bucket) interface{} {
	compiledJob, err := s.compiler.Compile(s.GetTemplate(), namespace, currentJobSpec)
	if err != nil {
		deployFailure := models.JobDeploymentFailure{
			JobName: currentJobSpec.Name,
			Message: err.Error(),
		}
		return deployFailure
	}

	blobKey := PathFromJobName(JobsDir, namespace.Name, compiledJob.Name, JobsExtension)
	if err := bucket.WriteAll(ctx, blobKey, compiledJob.Contents, nil); err != nil {
		deployFailure := models.JobDeploymentFailure{
			JobName: currentJobSpec.Name,
			Message: err.Error(),
		}
		return deployFailure
	}

	blobKeyNamespaceId := PathFromJobName(JobsDir, namespace.ID.String(), compiledJob.Name, JobsExtension)
	if err := bucket.Delete(ctx, blobKeyNamespaceId); err != nil {
		if err.Error() != "NotFound" {
			deployFailure := models.JobDeploymentFailure{
				JobName: currentJobSpec.Name,
				Message: "failed to cleanup old DAG::" + blobKeyNamespaceId + ", err::" + err.Error(),
			}
			return deployFailure
		}
	}
	err = deleteDirectoryIfEmpty(ctx, PathForJobDirectory(JobsDir, namespace.ID.String()), bucket)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == googleapi.ErrorItem. {

			}
		}
		if err.() != "NotFound" {
			deployFailure := models.JobDeploymentFailure{
				JobName: currentJobSpec.Name,
				Message: "failed to cleanup old dags folder " + err.Error(),
			}
			return deployFailure
		}
	}

	return nil
}

func (s *scheduler) DeleteJobs(ctx context.Context, namespace models.NamespaceSpec, jobNames []string, progressObserver progress.Observer) error {
	spanCtx, span := startChildSpan(ctx, "DeleteJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, namespace.ProjectSpec)
	if err != nil {
		return err
	}
	for _, jobName := range jobNames {
		if strings.TrimSpace(jobName) == "" {
			return ErrEmptyJobName
		}
		blobKey := PathFromJobName(JobsDir, namespace.ID.String(), jobName, JobsExtension)
		if err := bucket.Delete(spanCtx, blobKey); err != nil {
			// ignore missing files
			if gcerrors.Code(err) != gcerrors.NotFound {
				return err
			}
		}
		s.notifyProgress(progressObserver, &models.ProgressJobRemoteDelete{
			Name: jobName,
		})
	}
	return nil
}

// TODO list jobs should not refer from the scheduler, rather should list from db and it has notthing to do with scheduler.
func (s *scheduler) ListJobs(ctx context.Context, namespace models.NamespaceSpec, opts models.SchedulerListOptions) ([]models.Job, error) {
	spanCtx, span := startChildSpan(ctx, "ListJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, namespace.ProjectSpec)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	namespaceID := namespace.ID.String()
	var jobs []models.Job
	// get all items under namespace directory
	it := bucket.List(&blob.ListOptions{
		Prefix: PathForJobDirectory(JobsDir, namespaceID),
	})
	for {
		obj, err := it.Next(spanCtx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if strings.HasSuffix(obj.Key, JobsExtension) {
			jobs = append(jobs, models.Job{
				Name: JobNameFromPath(obj.Key, JobsExtension),
			})
		}
	}

	if opts.OnlyName {
		return jobs, nil
	}
	for idx, job := range jobs {
		jobs[idx].Contents, err = bucket.ReadAll(spanCtx, PathFromJobName(JobsDir, namespaceID, job.Name, JobsExtension))
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (s *scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobStatus")
	defer span.End()

	var jobStatus []models.JobStatus
	var list DagRunListResponse
	req := airflowRequest{
		URL:    dagStatusURL,
		method: http.MethodGet,
		param:  jobName,
		body:   nil,
	}
	resp, err := s.client.invoke(spanCtx, req, projSpec)
	if err != nil {
		return jobStatus, fmt.Errorf("failure reason for fetching airflow latest dag run: %w", err)
	}
	err = json.Unmarshal(resp, &list)
	if err != nil {
		return nil, fmt.Errorf("json error: %s : %w", string(resp), err)
	}
	return toJobStatus(list)
}

func (s *scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	spanCtx, span := startChildSpan(ctx, "Clear")
	defer span.End()

	data := []byte(fmt.Sprintf(`{"start_date": %q, "end_date": %q, "dry_run": false, "reset_dag_runs": true, "only_failed": false}`,
		startDate.UTC().Format(airflowDateFormat),
		endDate.UTC().Format(airflowDateFormat)))
	req := airflowRequest{
		URL:    dagRunClearURL,
		method: http.MethodPost,
		param:  jobName,
		body:   data,
	}
	_, err := s.client.invoke(spanCtx, req, projSpec)
	if err != nil {
		return fmt.Errorf("failure reason for clearing airflow dag runs: %w", err)
	}
	return nil
}

func (s *scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate, endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRunStatus")
	defer span.End()

	var jobStatus []models.JobStatus
	var list DagRunListResponse
	pageOffset := 0
	req := airflowRequest{
		URL:    dagStatusBatchURL,
		method: http.MethodPost,
		body:   []byte(""),
	}

	for {
		dagRunBatchReq := fmt.Sprintf(`{
		"page_offset": %d, 
		"page_limit": %d, 
		"dag_ids": ["%s"],
		"execution_date_gte": "%s",
		"execution_date_lte": "%s"
		}`, pageOffset, batchSize, jobName, startDate.UTC().Format(airflowDateFormat), endDate.UTC().Format(airflowDateFormat))
		req.body = []byte(dagRunBatchReq)
		resp, err := s.client.invoke(spanCtx, req, projectSpec)
		if err != nil {
			return nil, fmt.Errorf("failure reason for fetching airflow dag runs: %w", err)
		}
		if err := json.Unmarshal(resp, &list); err != nil {
			return nil, fmt.Errorf("json error: %s: %w", string(resp), err)
		}

		jobStatusPerBatch, err := toJobStatus(list)
		if err != nil {
			return nil, err
		}
		jobStatus = append(jobStatus, jobStatusPerBatch...)

		pageOffset += batchSize
		if list.TotalEntries <= pageOffset {
			break
		}
	}

	return jobStatus, nil
}

func (s *scheduler) GetJobRuns(ctx context.Context, projectSpec models.ProjectSpec, jobQuery *models.JobQuery, jobCron *cron.ScheduleSpec) ([]models.JobRun, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRuns")
	defer span.End()

	var jobRuns []models.JobRun
	var dagRunList DagRunListResponse
	dagRunRequest := s.GetDagRunRequest(jobQuery, jobCron)
	reqBody, err := json.Marshal(dagRunRequest)
	if err != nil {
		return jobRuns, err
	}
	req := airflowRequest{
		URL:    dagStatusBatchURL,
		method: http.MethodPost,
		body:   reqBody,
	}
	resp, err := s.client.invoke(spanCtx, req, projectSpec)
	if err != nil {
		return jobRuns, fmt.Errorf("failure reason for fetching airflow dag runs: %w", err)
	}
	if err := json.Unmarshal(resp, &dagRunList); err != nil {
		return jobRuns, fmt.Errorf("json error on parsing airflow dag runs: %s: %w", string(resp), err)
	}
	return getJobRuns(dagRunList, jobCron)
}

func (s *scheduler) GetDagRunRequest(jobQuery *models.JobQuery, jobCron *cron.ScheduleSpec) DagRunRequest {
	if jobQuery.OnlyLastRun {
		return DagRunRequest{
			OrderBy:    "-execution_date",
			PageOffset: 0,
			PageLimit:  1,
			DagIds:     []string{jobQuery.Name},
		}
	}
	startDate := s.getExecutionStartDate(jobQuery.StartDate, jobCron)
	endDate := s.getExecutionEndDate(jobQuery.EndDate, jobCron)
	return DagRunRequest{
		OrderBy:          "execution_date",
		PageOffset:       0,
		PageLimit:        pageLimit,
		DagIds:           []string{jobQuery.Name},
		ExecutionDateGte: startDate.Format(airflowDateFormat),
		ExecutionDateLte: endDate.Format(airflowDateFormat),
	}
}

func (*scheduler) getExecutionStartDate(scheduleStartTime time.Time, jobCron *cron.ScheduleSpec) time.Time {
	return jobCron.Prev(scheduleStartTime)
}

func (*scheduler) getExecutionEndDate(scheduleEndTime time.Time, jobCron *cron.ScheduleSpec) time.Time {
	// when the current time matches one of the schedule times execution time means previous schedule.
	if jobCron.Next(scheduleEndTime.Add(-time.Second * 1)).Equal(scheduleEndTime) {
		return jobCron.Prev(scheduleEndTime)
	}
	// else it is previous to previous schedule.
	return jobCron.Prev(jobCron.Prev(scheduleEndTime))
}

func (*scheduler) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

func PathForJobDirectory(prefix, namespace string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return path.Join(prefix, namespace)
}

func PathFromJobName(prefix, namespace, jobName, suffix string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return fmt.Sprintf("%s%s", path.Join(prefix, namespace, jobName), suffix)
}

func JobNameFromPath(filePath, suffix string) string {
	jobFileName := path.Base(filePath)
	return strings.TrimSuffix(jobFileName, suffix)
}

func NewScheduler(bucketFac BucketFactory, httpClient HTTPClient, compiler models.JobCompiler) *scheduler {
	return &scheduler{
		bucketFac: bucketFac,
		compiler:  compiler,
		client:    airflowClient{client: httpClient},
	}
}
