package airflow2

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

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
	dagStatusURL      = "api/v1/dags/%s/dagRuns?limit=99999"
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
	bucketFac  BucketFactory
	httpClient HTTPClient
	compiler   models.JobCompiler
}

func (s *scheduler) GetName() string {
	return "airflow2"
}

func (s *scheduler) GetTemplate() []byte {
	return resBaseDAG
}

func (s *scheduler) Bootstrap(ctx context.Context, proj models.ProjectSpec) error {
	bucket, err := s.bucketFac.New(ctx, proj)
	if err != nil {
		return err
	}
	defer bucket.Close()
	return bucket.WriteAll(ctx, filepath.Join(JobsDir, baseLibFileName), SharedLib, nil)
}

func (s *scheduler) VerifyJob(_ context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
	_, err := s.compiler.Compile(s.GetTemplate(), namespace, job)
	return err
}

func (s *scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec,
	progressObserver progress.Observer) error {
	bucket, err := s.bucketFac.New(ctx, namespace.ProjectSpec)
	if err != nil {
		return err
	}
	defer bucket.Close()

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, j := range jobs {
		runner.Add(func(currentJobSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				compiledJob, err := s.compiler.Compile(s.GetTemplate(), namespace, currentJobSpec)
				if err != nil {
					return nil, err
				}
				s.notifyProgress(progressObserver, &models.EventJobSpecCompiled{
					Name: compiledJob.Name,
				})

				blobKey := PathFromJobName(JobsDir, namespace.ID.String(), compiledJob.Name, JobsExtension)
				if err := bucket.WriteAll(ctx, blobKey, compiledJob.Contents, nil); err != nil {
					s.notifyProgress(progressObserver, &models.EventJobUpload{
						Name: compiledJob.Name,
						Err:  err,
					})
					return nil, err
				}
				s.notifyProgress(progressObserver, &models.EventJobUpload{
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

func (s *scheduler) DeleteJobs(ctx context.Context, namespace models.NamespaceSpec, jobNames []string,
	progressObserver progress.Observer) error {
	bucket, err := s.bucketFac.New(ctx, namespace.ProjectSpec)
	if err != nil {
		return err
	}
	for _, jobName := range jobNames {
		if strings.TrimSpace(jobName) == "" {
			return ErrEmptyJobName
		}
		blobKey := PathFromJobName(JobsDir, namespace.ID.String(), jobName, JobsExtension)
		if err := bucket.Delete(ctx, blobKey); err != nil {
			// ignore missing files
			if gcerrors.Code(err) != gcerrors.NotFound {
				return err
			}
		}
		s.notifyProgress(progressObserver, &models.EventJobRemoteDelete{
			Name: jobName,
		})
	}
	return nil
}

func (s *scheduler) ListJobs(ctx context.Context, namespace models.NamespaceSpec, opts models.SchedulerListOptions) ([]models.Job, error) {
	bucket, err := s.bucketFac.New(ctx, namespace.ProjectSpec)
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
		obj, err := it.Next(ctx)
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
		jobs[idx].Contents, err = bucket.ReadAll(ctx, PathFromJobName(JobsDir, namespaceID, job.Name, JobsExtension))
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (s *scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec,
	jobName string) ([]models.JobStatus, error) {
	schdHost, authToken, err := s.getHostAuth(projSpec)
	if err != nil {
		return nil, err
	}

	fetchURL := fmt.Sprintf(fmt.Sprintf("%s/%s", schdHost, dagStatusURL), jobName)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build http request for %s: %w", fetchURL, err)
	}
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(authToken))))

	resp, err := s.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch airflow dag runs from %s: %w", fetchURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch airflow dag runs from %s: %d", fetchURL, resp.StatusCode)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read airflow response: %w", err)
	}

	//{
	//	"dag_runs": [
	//		{
	//			"dag_id": "",
	//			"dag_run_url": "",
	//			"execution_date": "2020-03-25T02:00:00+00:00",
	//			"id": 1997,
	//			"run_id": "scheduled__2020-03-25T02:00:00+00:00",
	//			"start_date": "2020-06-01T16:32:58.489042+00:00",
	//			"state": "success"
	//	   },
	//	],
	//	"total_entries": 0
	//}
	var responseJSON struct {
		DagRuns []map[string]interface{} `json:"dag_runs"`
	}
	err = json.Unmarshal(body, &responseJSON)
	if err != nil {
		return nil, fmt.Errorf("json error: %s: %w", string(body), err)
	}

	return toJobStatus(responseJSON.DagRuns, jobName)
}

func (s *scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	schdHost, authToken, err := s.getHostAuth(projSpec)
	if err != nil {
		return err
	}

	schdHost = strings.Trim(schdHost, "/")
	jsonStr := []byte(fmt.Sprintf(`{"start_date":"%s", "end_date": "%s", "dry_run": false, "reset_dag_runs": true, "only_failed": false}`,
		startDate.UTC().Format(airflowDateFormat),
		endDate.UTC().Format(airflowDateFormat)))
	postURL := fmt.Sprintf(
		fmt.Sprintf("%s/%s", schdHost, dagRunClearURL),
		jobName)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewBuffer(jsonStr))
	if err != nil {
		return fmt.Errorf("failed to build http request for %s: %w", postURL, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(authToken))))

	resp, err := s.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to clear airflow dag runs from %s: %w", postURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to clear airflow dag runs from %s: %d", postURL, resp.StatusCode)
	}
	defer resp.Body.Close()

	return nil
}

func (s *scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate time.Time,
	endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	schdHost, authToken, err := s.getHostAuth(projectSpec)
	if err != nil {
		return nil, err
	}
	schdHost = strings.Trim(schdHost, "/")
	postURL := fmt.Sprintf("%s/%s", schdHost, dagStatusBatchURL)

	pageOffset := 0
	var jobStatus []models.JobStatus
	var responseJSON struct {
		DagRuns      []map[string]interface{} `json:"dag_runs"`
		TotalEntries int                      `json:"total_entries"`
	}

	for {
		dagRunBatchReq := fmt.Sprintf(`{
		"page_offset": %d, 
		"page_limit": %d, 
		"dag_ids": ["%s"],
		"execution_date_gte": "%s",
		"execution_date_lte": "%s"
		}`, pageOffset, batchSize, jobName, startDate.UTC().Format(airflowDateFormat), endDate.UTC().Format(airflowDateFormat))
		jsonStr := []byte(dagRunBatchReq)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewBuffer(jsonStr))
		if err != nil {
			return nil, fmt.Errorf("failed to build http request for %s: %w", dagStatusBatchURL, err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(authToken))))

		resp, err := s.httpClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch airflow dag runs from %s: %w", dagStatusBatchURL, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to fetch airflow dag runs from %s", dagStatusBatchURL)
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read airflow response: %w", err)
		}

		if err := json.Unmarshal(body, &responseJSON); err != nil {
			return nil, fmt.Errorf("json error: %s: %w", string(body), err)
		}

		jobStatusPerBatch, err := toJobStatus(responseJSON.DagRuns, jobName)
		if err != nil {
			return nil, err
		}
		jobStatus = append(jobStatus, jobStatusPerBatch...)

		pageOffset += batchSize
		if responseJSON.TotalEntries <= pageOffset {
			break
		}
	}

	return jobStatus, nil
}

func (s *scheduler) getHostAuth(projectSpec models.ProjectSpec) (string, string, error) {
	schdHost, ok := projectSpec.Config[models.ProjectSchedulerHost]
	if !ok {
		return "", "", fmt.Errorf("scheduler host not set for %s", projectSpec.Name)
	}
	authToken, ok := projectSpec.Secret.GetByName(models.ProjectSchedulerAuth)
	if !ok {
		return "", "", fmt.Errorf("%s secret not configured for project %s", models.ProjectSchedulerAuth, projectSpec.Name)
	}
	return schdHost, authToken, nil
}

func (s *scheduler) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

func toJobStatus(dagRuns []map[string]interface{}, jobName string) ([]models.JobStatus, error) {
	var jobStatus []models.JobStatus
	for _, status := range dagRuns {
		_, ok1 := status["execution_date"]
		_, ok2 := status["state"]
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("failed to find required response fields %s in %s", jobName, status)
		}
		scheduledAt, err := time.Parse(models.InstanceScheduledAtTimeLayout, status["execution_date"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing date for %s, %s", jobName, status["execution_date"].(string))
		}
		jobStatus = append(jobStatus, models.JobStatus{
			ScheduledAt: scheduledAt,
			State:       models.JobRunState(status["state"].(string)),
		})
	}
	return jobStatus, nil
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
		bucketFac:  bucketFac,
		compiler:   compiler,
		httpClient: httpClient,
	}
}
