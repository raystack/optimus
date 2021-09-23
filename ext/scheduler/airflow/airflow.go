package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/core/progress"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/odpf/optimus/ext/scheduler/airflow2"

	_ "embed"

	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

//go:embed resources/__lib.py
var SharedLib []byte

//go:embed resources/base_dag.py
var resBaseDAG []byte

const (
	baseLibFileName = "__lib.py"
	dagStatusURL    = "api/experimental/dags/%s/dag_runs"
	dagRunClearURL  = "clear&dag_id=%s&start_date=%s&end_date=%s"

	JobsDir       = "dags"
	JobsExtension = ".py"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type scheduler struct {
	bucketFac  airflow2.BucketFactory
	httpClient HTTPClient
	compiler   models.JobCompiler
}

func NewScheduler(bf airflow2.BucketFactory, httpClient HTTPClient, compiler models.JobCompiler) *scheduler {
	return &scheduler{
		bucketFac:  bf,
		httpClient: httpClient,
		compiler:   compiler,
	}
}

func (a *scheduler) GetName() string {
	return "airflow"
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

func (s *scheduler) VerifyJob(ctx context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
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

	runner := parallel.NewRunner(parallel.WithTicket(airflow2.ConcurrentTicketPerSec), parallel.WithLimit(airflow2.ConcurrentLimit))
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

				blobKey := airflow2.PathFromJobName(JobsDir, namespace.ID.String(), compiledJob.Name, JobsExtension)
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
			return airflow2.ErrEmptyJobName
		}
		blobKey := airflow2.PathFromJobName(JobsDir, namespace.ID.String(), jobName, JobsExtension)
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

	var jobs []models.Job
	// get all items
	it := bucket.List(&blob.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if strings.HasSuffix(obj.Key, JobsExtension) {
			jobs = append(jobs, models.Job{
				Name: airflow2.JobNameFromPath(obj.Key, JobsExtension),
			})
		}
	}

	if opts.OnlyName {
		return jobs, nil
	}
	for idx, job := range jobs {
		jobs[idx].Contents, err = bucket.ReadAll(ctx, airflow2.PathFromJobName(JobsDir, namespace.ID.String(), job.Name, JobsExtension))
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (a *scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus,
	error) {
	schdHost, ok := projSpec.Config[models.ProjectSchedulerHost]
	if !ok {
		return nil, errors.Errorf("scheduler host not set for %s", projSpec.Name)
	}
	schdHost = strings.Trim(schdHost, "/")

	fetchURL := fmt.Sprintf(fmt.Sprintf("%s/%s", schdHost, dagStatusURL), jobName)
	request, err := http.NewRequest(http.MethodGet, fetchURL, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build http request for %s", fetchURL)
	}

	resp, err := a.httpClient.Do(request)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch airflow dag runs from %s", fetchURL)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to fetch airflow dag runs from %s: %d", fetchURL, resp.StatusCode)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read airflow response")
	}

	//{
	//	"dag_id": "",
	//	"dag_run_url": "",
	//	"execution_date": "2020-03-25T02:00:00+00:00",
	//	"id": 1997,
	//	"run_id": "scheduled__2020-03-25T02:00:00+00:00",
	//	"start_date": "2020-06-01T16:32:58.489042+00:00",
	//	"state": "success"
	//},
	responseJSON := []map[string]interface{}{}
	err = json.Unmarshal(body, &responseJSON)
	if err != nil {
		return nil, errors.Wrapf(err, "json error: %s", string(body))
	}

	jobStatus := []models.JobStatus{}
	for _, status := range responseJSON {
		_, ok1 := status["execution_date"]
		_, ok2 := status["state"]
		if !ok1 || !ok2 {
			return nil, errors.Errorf("failed to find required response fields %s in %s", jobName, status)
		}
		schdAt, err := time.Parse(models.InstanceScheduledAtTimeLayout, status["execution_date"].(string))
		if err != nil {
			return nil, errors.Errorf("error parsing date for %s, %s", jobName, status["execution_date"].(string))
		}
		jobStatus = append(jobStatus, models.JobStatus{
			ScheduledAt: schdAt,
			State:       models.JobRunState(status["state"].(string)),
		})
	}

	return jobStatus, nil
}

func (a *scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	schdHost, ok := projSpec.Config[models.ProjectSchedulerHost]
	if !ok {
		return errors.Errorf("scheduler host not set for %s", projSpec.Name)
	}

	schdHost = strings.Trim(schdHost, "/")
	airflowDateFormat := "2006-01-02T15:04:05"
	utcTimezone, _ := time.LoadLocation("UTC")
	clearDagRunURL := fmt.Sprintf(
		fmt.Sprintf("%s/%s", schdHost, dagRunClearURL),
		jobName,
		startDate.In(utcTimezone).Format(airflowDateFormat),
		endDate.In(utcTimezone).Format(airflowDateFormat))
	request, err := http.NewRequest(http.MethodGet, clearDagRunURL, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to build http request for %s", clearDagRunURL)
	}

	resp, err := a.httpClient.Do(request)
	if err != nil {
		return errors.Wrapf(err, "failed to clear airflow dag runs from %s", clearDagRunURL)
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("failed to clear airflow dag runs from %s: %d", clearDagRunURL, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read airflow response")
	}

	//{
	//	"http_response_code": 200,
	//	"status": "status"
	//}
	responseJSON := map[string]interface{}{}
	err = json.Unmarshal(body, &responseJSON)
	if err != nil {
		return errors.Wrapf(err, "json error: %s", string(body))
	}

	responseFields := []string{"http_response_code", "status"}
	for _, field := range responseFields {
		if _, ok := responseJSON[field]; !ok {
			return errors.Errorf("failed to find required response fields %s in %s", field, responseJSON)
		}
	}
	return nil
}

func (a *scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate time.Time, endDate time.Time,
	batchSize int) ([]models.JobStatus, error) {
	allJobStatus, err := a.GetJobStatus(ctx, projectSpec, jobName)
	if err != nil {
		return nil, err
	}

	var requestedJobStatus []models.JobStatus
	for _, jobStatus := range allJobStatus {
		if jobStatus.ScheduledAt.Equal(startDate) || (jobStatus.ScheduledAt.After(startDate) && jobStatus.ScheduledAt.Before(endDate)) {
			requestedJobStatus = append(requestedJobStatus, jobStatus)
		}
	}

	return requestedJobStatus, nil
}

func (s *scheduler) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}
