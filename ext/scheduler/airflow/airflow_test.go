package airflow_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"

	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
)

type MockHTTPClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if m.DoFunc != nil {
		return m.DoFunc(req)
	}
	// default if none provided
	return &http.Response{}, nil
}

type MockedBucketFactory struct {
	mock.Mock
}

func (m *MockedBucketFactory) New(ctx context.Context, proj models.ProjectSpec) (airflow2.Bucket, error) {
	args := m.Called(ctx, proj)
	return args.Get(0).(airflow2.Bucket), args.Error(1)
}

type MockedBucket struct {
	mock.Mock
	// inmemory
	bucket *blob.Bucket
}

func (m *MockedBucket) WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error {
	_ = m.Called(ctx, key, p, opts)
	return m.bucket.WriteAll(ctx, key, p, opts)
}

func (m *MockedBucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	_ = m.Called(ctx, key)
	return m.bucket.ReadAll(ctx, key)
}

func (m *MockedBucket) List(opts *blob.ListOptions) *blob.ListIterator {
	_ = m.Called(opts)
	return m.bucket.List(opts)
}

func (m *MockedBucket) Delete(ctx context.Context, key string) error {
	_ = m.Called(ctx, key)
	return m.bucket.Delete(ctx, key)
}

func (m *MockedBucket) Close() error {
	return nil
}

type MockedCompiler struct {
	mock.Mock
}

func (srv *MockedCompiler) Compile(template []byte, namespace models.NamespaceSpec, jobSpec models.JobSpec) (models.Job, error) {
	args := srv.Called(template, namespace, jobSpec)
	return args.Get(0).(models.Job), args.Error(1)
}

func TestAirflow(t *testing.T) {
	ctx := context.Background()
	proj := models.ProjectSpec{
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
	}
	nsUUID := uuid.Must(uuid.NewRandom())
	ns := models.NamespaceSpec{
		ID:          nsUUID,
		Name:        "local-namespace",
		ProjectSpec: proj,
	}
	jobSpecs := []models.JobSpec{
		{
			Name: "job-1",
		},
	}
	t.Run("Bootstrap", func(t *testing.T) {
		t.Run("should successfully bootstrap for gcs buckets", func(t *testing.T) {
			inmemBlob := memblob.OpenBucket(nil)
			mbucket := &MockedBucket{
				bucket: inmemBlob,
			}
			defer mbucket.AssertExpectations(t)

			mbucketFac := new(MockedBucketFactory)
			mbucketFac.On("New", ctx, proj).Return(mbucket, nil)
			defer mbucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mbucketFac, nil, nil)
			mbucket.On("WriteAll", ctx, "dags/__lib.py", airflow.SharedLib, (*blob.WriterOptions)(nil)).Return(nil)
			err := air.Bootstrap(ctx, proj)
			assert.Nil(t, err)

			storedBytes, err := inmemBlob.ReadAll(ctx, "dags/__lib.py")
			assert.Nil(t, err)
			assert.Equal(t, airflow.SharedLib, storedBytes)
		})
	})
	t.Run("DeployJobs", func(t *testing.T) {
		t.Run("should successfully deploy jobs to blob buckets", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			compiler := new(MockedCompiler)
			air := airflow.NewScheduler(mockBucketFac, nil, compiler)
			defer compiler.AssertExpectations(t)

			compiler.On("Compile", air.GetTemplate(), ns, jobSpecs[0]).Return(models.Job{
				Name:     jobSpecs[0].Name,
				Contents: []byte("job-1-compiled"),
			}, nil)

			mockBucket.On("WriteAll", ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name), []byte("job-1-compiled"), (*blob.WriterOptions)(nil)).Return(nil)
			err := air.DeployJobs(ctx, ns, jobSpecs, nil)
			assert.Nil(t, err)

			storedBytes, err := inMemBlob.ReadAll(ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name))
			assert.Nil(t, err)
			assert.Equal(t, []byte("job-1-compiled"), storedBytes)
		})
	})
	t.Run("DeleteJobs", func(t *testing.T) {
		t.Run("should successfully delete jobs from blob buckets", func(t *testing.T) {
			jobKey := fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name)

			inMemBlob := memblob.OpenBucket(nil)
			_ = inMemBlob.WriteAll(ctx, jobKey, []byte("hello"), nil)

			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			mockBucket.On("Delete", ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name)).Return(nil)
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mockBucketFac, nil, nil)
			err := air.DeleteJobs(ctx, ns, []string{"job-1"}, nil)
			assert.Nil(t, err)

			jobStillExist, err := inMemBlob.Exists(ctx, jobKey)
			assert.Nil(t, err)
			assert.Equal(t, false, jobStillExist)
		})
		t.Run("should silently ignore delete operation on missing job from blob buckets", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			mockBucket.On("Delete", ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name)).Return(nil)
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mockBucketFac, nil, nil)
			err := air.DeleteJobs(ctx, ns, []string{"job-1"}, nil)
			assert.Nil(t, err)
		})
	})
	t.Run("ListJobs", func(t *testing.T) {
		t.Run("should only list item names if asked to skip content", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			_ = inMemBlob.WriteAll(ctx, filepath.Join(airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()), "file1.py"), []byte("test1"), nil)
			_ = inMemBlob.WriteAll(ctx, filepath.Join(airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()), "file2.py"), []byte("test2"), nil)
			_ = inMemBlob.WriteAll(ctx, "bar.py", []byte("test3"), nil)
			mockBucket.On("List", &blob.ListOptions{
				Prefix: airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()),
			})
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mockBucketFac, nil, nil)
			respJobs, err := air.ListJobs(ctx, ns, models.SchedulerListOptions{OnlyName: true})
			assert.Nil(t, err)
			assert.Equal(t, 2, len(respJobs))
		})
		t.Run("should only list item names if extension matches requested job", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			_ = inMemBlob.WriteAll(ctx, filepath.Join(airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()), "file1.py"), []byte("test1"), nil)
			_ = inMemBlob.WriteAll(ctx, filepath.Join(airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()), "file2.json"), []byte("test2"), nil)
			_ = inMemBlob.WriteAll(ctx, "bar.py", []byte("test3"), nil)
			mockBucket.On("List", &blob.ListOptions{
				Prefix: airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()),
			})
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mockBucketFac, nil, nil)
			respJobs, err := air.ListJobs(ctx, ns, models.SchedulerListOptions{OnlyName: true})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(respJobs))
		})
		t.Run("should list item with name and content correctly", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			_ = inMemBlob.WriteAll(ctx, airflow2.PathFromJobName(airflow.JobsDir, ns.ID.String(), "file1", airflow.JobsExtension), []byte("test1"), nil)
			_ = inMemBlob.WriteAll(ctx, airflow2.PathFromJobName(airflow.JobsDir, ns.ID.String(), "file2", airflow.JobsExtension), []byte("test2"), nil)
			mockBucket.On("List", &blob.ListOptions{
				Prefix: airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()),
			})
			mockBucket.On("ReadAll", ctx, airflow2.PathFromJobName(airflow.JobsDir, ns.ID.String(), "file1", airflow.JobsExtension))
			mockBucket.On("ReadAll", ctx, airflow2.PathFromJobName(airflow.JobsDir, ns.ID.String(), "file2", airflow.JobsExtension))
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow.NewScheduler(mockBucketFac, nil, nil)
			respJobs, err := air.ListJobs(ctx, ns, models.SchedulerListOptions{})
			assert.Nil(t, err)
			assert.Equal(t, 2, len(respJobs))
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
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
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
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
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
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
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
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
			err := air.Clear(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetJobRunStatus", func(t *testing.T) {
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
			expectedExecutionTime0, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-25T02:00:00+00:00")
			expectedExecutionTime1, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-26T02:00:00+00:00")
			expectedStatus := []models.JobStatus{
				{
					ScheduledAt: expectedExecutionTime0,
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: expectedExecutionTime1,
					State:       models.RunStateRunning,
				},
			}

			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
			status, err := air.GetJobRunStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select", startDateTime, endDateTime, 0)

			assert.Nil(t, err)
			assert.Equal(t, expectedStatus, status)
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
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow.NewScheduler(nil, client, nil)
			status, err := air.GetJobRunStatus(ctx, models.ProjectSpec{
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
