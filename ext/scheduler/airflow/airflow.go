package airflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	_ "embed"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

//go:embed resources/__lib.py
var resSharedLib []byte

//go:embed resources/base_dag.py
var resBaseDAG []byte

const (
	baseLibFileName = "__lib.py"
	dagStatusURL    = "api/experimental/dags/%s/dag_runs"
	dagRunClearURL  = "clear&dag_id=%s&start_date=%s&end_date=%s"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type ObjectWriterFactory interface {
	New(ctx context.Context, writerPath, writerSecret string) (store.ObjectWriter, error)
}

type scheduler struct {
	objWriterFac ObjectWriterFactory
	httpClient   HTTPClient
}

func NewScheduler(ow ObjectWriterFactory, httpClient HTTPClient) *scheduler {
	return &scheduler{
		objWriterFac: ow,
		httpClient:   httpClient,
	}
}

func (a *scheduler) GetName() string {
	return "airflow"
}

func (a *scheduler) GetJobsDir() string {
	return "dags"
}

func (a *scheduler) GetJobsExtension() string {
	return ".py"
}

func (a *scheduler) GetTemplate() []byte {
	return resBaseDAG
}

func (a *scheduler) Bootstrap(ctx context.Context, proj models.ProjectSpec) error {
	storagePath, ok := proj.Config[models.ProjectStoragePathKey]
	if !ok {
		return errors.Errorf("%s config not configured for project %s", models.ProjectStoragePathKey, proj.Name)
	}
	storageSecret, ok := proj.Secret.GetByName(models.ProjectSecretStorageKey)
	if !ok {
		return errors.Errorf("%s secret not configured for project %s", models.ProjectSecretStorageKey, proj.Name)
	}

	p, err := url.Parse(storagePath)
	if err != nil {
		return err
	}
	objectWriter, err := a.objWriterFac.New(ctx, storagePath, storageSecret)
	if err != nil {
		return errors.Errorf("object writer failed for %s", proj.Name)
	}
	return a.migrateLibFileToWriter(ctx, objectWriter, p.Hostname(), filepath.Join(strings.Trim(p.Path, "/"), a.GetJobsDir(), baseLibFileName))
}

func (a *scheduler) migrateLibFileToWriter(ctx context.Context, objWriter store.ObjectWriter, bucket, objDir string) (err error) {
	// copy to fs
	dst, err := objWriter.NewWriter(ctx, bucket, objDir)
	if err != nil {
		return err
	}
	defer func() {
		if derr := dst.Close(); derr != nil {
			if err == nil {
				err = derr
			} else {
				err = errors.Wrap(err, derr.Error())
			}
		}
	}()

	_, err = io.Copy(dst, bytes.NewBuffer(resSharedLib))
	return
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
			State:       models.JobStatusState(status["state"].(string)),
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

func (a *scheduler) GetDagRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate time.Time, endDate time.Time,
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
