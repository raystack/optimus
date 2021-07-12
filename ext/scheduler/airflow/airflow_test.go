package airflow_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/odpf/optimus/job"

	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/scheduler/airflow"
	mocked "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

type MockHttpClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	if m.DoFunc != nil {
		return m.DoFunc(req)
	}
	// default if none provided
	return &http.Response{}, nil
}

type MockedObjectWriterFactory struct {
	mock.Mock
}

func (m *MockedObjectWriterFactory) New(ctx context.Context, path, writerSecret string) (store.ObjectWriter, error) {
	args := m.Called(ctx, path, writerSecret)
	return args.Get(0).(store.ObjectWriter), args.Error(1)
}

func TestAirflow(t *testing.T) {
	ctx := context.Background()
	t.Run("Bootstrap", func(t *testing.T) {
		t.Run("should successfully bootstrap for gcs buckets", func(t *testing.T) {
			var out bytes.Buffer
			wc := new(mocked.WriteCloser)
			defer wc.AssertExpectations(t)
			wc.On("Write").Return(&out, nil)
			wc.On("Close").Return(nil)

			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			owf := new(MockedObjectWriterFactory)
			owf.On("New", ctx, "gs://mybucket/hello", "test-secret").Return(ow, nil)
			defer owf.AssertExpectations(t)

			bucket := "mybucket"
			objectPath := fmt.Sprintf("hello/%s/%s", "dags", "__lib.py")
			ow.On("NewWriter", ctx, bucket, objectPath).Return(wc, nil)

			air := airflow.NewScheduler(owf, nil)
			err := air.Bootstrap(context.Background(), models.ProjectSpec{
				Name: "proj-name",
				Config: map[string]string{
					models.ProjectStoragePathKey: "gs://mybucket/hello",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSecretStorageKey,
						Value: "test-secret",
					},
				},
			})
			assert.Nil(t, err)
		})
		t.Run("should fail if no storage config is set", func(t *testing.T) {
			air := airflow.NewScheduler(nil, nil)
			err := air.Bootstrap(ctx, models.ProjectSpec{
				Name:   "proj-name",
				Config: map[string]string{},
			})
			assert.NotNil(t, err)
		})
		t.Run("should fail for unsupported storage interfaces", func(t *testing.T) {
			air := airflow.NewScheduler(nil, nil)
			err := air.Bootstrap(ctx, models.ProjectSpec{
				Name: "proj-name",
				Config: map[string]string{
					models.ProjectStoragePathKey: "xxx://mybucket/dags",
				},
			})
			assert.NotNil(t, err)
		})
	})
	t.Run("GetJobStatus", func(t *testing.T) {
		host := "http://airflow.example.io"

		t.Run("should return job status with valid args", func(t *testing.T) {
			respString := `
[
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-25+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-25T02:00:00+00:00",
	"id": 1997,
	"run_id": "scheduled__2020-03-25T02:00:00+00:00",
	"start_date": "2020-06-01T16:32:58.489042+00:00",
	"state": "success"
},
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-26+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-26T02:00:00+00:00",
	"id": 1998,
	"run_id": "scheduled__2020-03-26T02:00:00+00:00",
	"start_date": "2020-06-01T16:33:01.020645+00:00",
	"state": "success"
}
]`
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			status, err := air.GetJobStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select")

			assert.Nil(t, err)
			assert.Len(t, status, 2)
		})
		t.Run("should fail if host fails to return OK", func(t *testing.T) {
			respString := `INTERNAL ERROR`
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			status, err := air.GetJobStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select")

			assert.NotNil(t, err)
			assert.Len(t, status, 0)
		})
	})
	t.Run("Clear", func(t *testing.T) {
		host := "http://airflow.example.io"
		startDate := "2021-05-20"
		startDateTime, _ := time.Parse(job.ReplayDateFormat, startDate)
		endDate := "2021-05-25"
		endDateTime, _ := time.Parse(job.ReplayDateFormat, endDate)

		t.Run("should return job status with valid args", func(t *testing.T) {
			respString := `
{
	"http_response_code": 200,
	"status": "success"
}`
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			err := air.Clear(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime)

			assert.Nil(t, err)
		})
		t.Run("should fail if host fails to return OK", func(t *testing.T) {
			respString := `INTERNAL ERROR`
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			err := air.Clear(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetDagRunStatus", func(t *testing.T) {
		host := "http://airflow.example.io"
		startDate := "2020-03-25"
		startDateTime, _ := time.Parse(job.ReplayDateFormat, startDate)
		endDate := "2020-03-27"
		endDateTime, _ := time.Parse(job.ReplayDateFormat, endDate)
		t.Run("should return dag run status list with valid args", func(t *testing.T) {
			respString := `
[
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-25+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-25T02:00:00+00:00",
	"id": 1997,
	"run_id": "scheduled__2020-03-25T02:00:00+00:00",
	"start_date": "2020-06-01T16:32:58.489042+00:00",
	"state": "success"
},
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-26+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-26T02:00:00+00:00",
	"id": 1998,
	"run_id": "scheduled__2020-03-26T02:00:00+00:00",
	"start_date": "2020-06-01T16:33:01.020645+00:00",
	"state": "running"
}
]`
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			status, err := air.GetDagRunStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime, 0)

			assert.Nil(t, err)
			assert.Len(t, status, 2)
		})
		t.Run("should not return any status if no dag run found for the requested window", func(t *testing.T) {
			respString := `
[
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-25+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-01T02:00:00+00:00",
	"id": 1997,
	"run_id": "scheduled__2020-03-25T02:00:00+00:00",
	"start_date": "2020-06-01T16:32:58.489042+00:00",
	"state": "success"
},
{
	"dag_id": "sample_select",
	"dag_run_url": "/graph?dag_id=sample_select&execution_date=2020-03-26+02%3A00%3A00%2B00%3A00",
	"execution_date": "2020-03-02T02:00:00+00:00",
	"id": 1998,
	"run_id": "scheduled__2020-03-26T02:00:00+00:00",
	"start_date": "2020-06-01T16:33:01.020645+00:00",
	"state": "running"
}
]`
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client)
			status, err := air.GetDagRunStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime, 0)

			assert.Nil(t, err)
			assert.Len(t, status, 0)
		})
	})
}
