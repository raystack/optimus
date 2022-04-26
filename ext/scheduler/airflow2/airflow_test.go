package airflow2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/odpf/optimus/core/cron"
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

func (*MockedBucket) Close() error {
	return nil
}

type MockedCompiler struct {
	mock.Mock
}

func (srv *MockedCompiler) Compile(template []byte, namespace models.NamespaceSpec, jobSpec models.JobSpec) (models.Job, error) {
	args := srv.Called(template, namespace, jobSpec)
	return args.Get(0).(models.Job), args.Error(1)
}

func TestAirflow2(t *testing.T) {
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
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
			mockBucket.On("WriteAll", ctx, "dags/__lib.py", airflow2.SharedLib, (*blob.WriterOptions)(nil)).Return(nil)
			err := air.Bootstrap(ctx, proj)
			assert.Nil(t, err)

			storedBytes, err := inMemBlob.ReadAll(ctx, "dags/__lib.py")
			assert.Nil(t, err)
			assert.Equal(t, airflow2.SharedLib, storedBytes)
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
			air := airflow2.NewScheduler(mockBucketFac, nil, compiler)
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
	t.Run("DeployJobsVerbose", func(t *testing.T) {
		t.Run("should successfully deploy jobs to blob buckets", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			defer mockBucketFac.AssertExpectations(t)

			compiler := new(MockedCompiler)
			defer compiler.AssertExpectations(t)

			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)

			air := airflow2.NewScheduler(mockBucketFac, nil, compiler)
			compiler.On("Compile", air.GetTemplate(), ns, jobSpecs[0]).Return(models.Job{
				Name:     jobSpecs[0].Name,
				Contents: []byte("job-1-compiled"),
			}, nil)

			mockBucket.On("WriteAll", ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name), []byte("job-1-compiled"), (*blob.WriterOptions)(nil)).Return(nil)

			expectedDeployDetail := models.JobDeploymentDetail{
				SuccessCount: 1,
			}

			actualDeployDetail, err := air.DeployJobsVerbose(ctx, ns, jobSpecs)
			assert.Nil(t, err)
			assert.Equal(t, expectedDeployDetail, actualDeployDetail)

			storedBytes, err := inMemBlob.ReadAll(ctx, fmt.Sprintf("dags/%s/%s.py", nsUUID, jobSpecs[0].Name))
			assert.Nil(t, err)
			assert.Equal(t, []byte("job-1-compiled"), storedBytes)
		})
		t.Run("should failed when unable to get bucket for the requested project", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			defer mockBucketFac.AssertExpectations(t)

			compiler := new(MockedCompiler)
			defer compiler.AssertExpectations(t)

			errorMsg := "internal error"
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, errors.New(errorMsg))

			air := airflow2.NewScheduler(mockBucketFac, nil, compiler)
			actualDeployDetail, err := air.DeployJobsVerbose(ctx, ns, jobSpecs)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeploymentDetail{}, actualDeployDetail)
		})
		t.Run("should able to add any compilation failures to response detail", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			defer mockBucketFac.AssertExpectations(t)

			compiler := new(MockedCompiler)
			defer compiler.AssertExpectations(t)

			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)

			errorMsg := "internal error"
			air := airflow2.NewScheduler(mockBucketFac, nil, compiler)
			compiler.On("Compile", air.GetTemplate(), ns, jobSpecs[0]).Return(models.Job{}, errors.New(errorMsg))

			expectedDeployDetail := models.JobDeploymentDetail{
				SuccessCount: 0,
				FailureCount: 1,
				Failures: []models.JobDeploymentFailure{
					{
						JobName: jobSpecs[0].Name,
						Message: errorMsg,
					},
				},
			}

			actualDeployDetail, err := air.DeployJobsVerbose(ctx, ns, jobSpecs)
			assert.Nil(t, err)
			assert.Equal(t, expectedDeployDetail, actualDeployDetail)
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

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
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

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
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

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
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

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
			respJobs, err := air.ListJobs(ctx, ns, models.SchedulerListOptions{OnlyName: true})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(respJobs))
		})
		t.Run("should list item with name and content correctly", func(t *testing.T) {
			inMemBlob := memblob.OpenBucket(nil)
			mockBucket := &MockedBucket{
				bucket: inMemBlob,
			}
			_ = inMemBlob.WriteAll(ctx, airflow2.PathFromJobName(airflow2.JobsDir, ns.ID.String(), "file1", airflow2.JobsExtension), []byte("test1"), nil)
			_ = inMemBlob.WriteAll(ctx, airflow2.PathFromJobName(airflow2.JobsDir, ns.ID.String(), "file2", airflow2.JobsExtension), []byte("test2"), nil)
			mockBucket.On("List", &blob.ListOptions{
				Prefix: airflow2.PathForJobDirectory(airflow2.JobsDir, ns.ID.String()),
			})
			mockBucket.On("ReadAll", ctx, airflow2.PathFromJobName(airflow2.JobsDir, ns.ID.String(), "file1", airflow2.JobsExtension))
			mockBucket.On("ReadAll", ctx, airflow2.PathFromJobName(airflow2.JobsDir, ns.ID.String(), "file2", airflow2.JobsExtension))
			defer mockBucket.AssertExpectations(t)

			mockBucketFac := new(MockedBucketFactory)
			mockBucketFac.On("New", ctx, proj).Return(mockBucket, nil)
			defer mockBucketFac.AssertExpectations(t)

			air := airflow2.NewScheduler(mockBucketFac, nil, nil)
			respJobs, err := air.ListJobs(ctx, ns, models.SchedulerListOptions{})
			assert.Nil(t, err)
			assert.Equal(t, 2, len(respJobs))
		})
	})

	t.Run("GetJobStatus", func(t *testing.T) {
		host := "http://airflow.example.io"

		t.Run("should return job status with valid args", func(t *testing.T) {
			respString := `
{
"dag_runs": [
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
],
"total_entries": 0
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

			air := airflow2.NewScheduler(nil, client, nil)
			status, err := air.GetJobStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
					models.ProjectSchedulerAuth: "admin:admin",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
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

			air := airflow2.NewScheduler(nil, client, nil)
			status, err := air.GetJobStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, "sample_select")

			assert.NotNil(t, err)
			assert.Len(t, status, 0)
		})
		t.Run("should fail if not scheduler secret registered", func(t *testing.T) {
			air := airflow2.NewScheduler(nil, nil, nil)
			_, err := air.GetJobStatus(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, "sample_select")
			assert.NotNil(t, err)
		})
	})
	t.Run("Clear", func(t *testing.T) {
		host := "http://airflow.example.io"
		startDate := "2021-05-20"
		startDateTime, _ := time.Parse(job.ReplayDateFormat, startDate)
		endDate := "2021-05-25"
		endDateTime, _ := time.Parse(job.ReplayDateFormat, endDate)

		t.Run("should clear dagrun state successfully", func(t *testing.T) {
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte("")))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			err := air.Clear(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
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

			air := airflow2.NewScheduler(nil, client, nil)
			err := air.Clear(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, "sample_select", startDateTime, endDateTime)

			assert.NotNil(t, err)
		})
		t.Run("should fail if not scheduler secret registered", func(t *testing.T) {
			air := airflow2.NewScheduler(nil, nil, nil)
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
		startDate := "2021-05-20"
		startDateTime, _ := time.Parse(job.ReplayDateFormat, startDate)
		endDate := "2021-05-25"
		endDateTime, _ := time.Parse(job.ReplayDateFormat, endDate)
		batchSize := 2
		projectSpec := models.ProjectSpec{
			Name: "test-proj",
			Config: map[string]string{
				models.ProjectSchedulerHost: host,
			},
			Secret: []models.ProjectSecretItem{
				{
					Name:  models.ProjectSchedulerAuth,
					Value: "admin:admin",
				},
			},
		}
		jobName := "sample_select"

		t.Run("should return dag run status with valid args", func(t *testing.T) {
			respString := `
{
"dag_runs": [
	{
		"conf": {},
        "dag_id": "sample_select",
        "dag_run_id": "scheduled__2020-03-25T02:00:00+00:00",
        "end_date": "2020-06-01T17:32:58.489042+00:00",
        "execution_date": "2020-03-25T02:00:00+00:00",
        "external_trigger": false,
        "start_date": "2020-06-01T16:32:58.489042+00:00",
        "state": "success"
	},
	{
		"conf": {},
        "dag_id": "sample_select",
        "dag_run_id": "scheduled__2020-03-26T02:00:00+00:00",
        "end_date": "2020-06-01T16:33:01.020645+00:00",
        "execution_date": "2020-03-26T02:00:00+00:00",
        "external_trigger": false,
        "start_date": "2020-06-01T16:33:01.020645+00:00",
        "state": "success"
	}
],
"total_entries": 2
}`
			expectedExecutionTime0, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-25T02:00:00+00:00")
			expectedExecutionTime1, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-26T02:00:00+00:00")
			expectedStatus := []models.JobStatus{
				{
					ScheduledAt: expectedExecutionTime0,
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: expectedExecutionTime1,
					State:       models.RunStateSuccess,
				},
			}

			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			status, err := air.GetJobRunStatus(ctx, projectSpec, jobName, startDateTime, endDateTime, batchSize)

			assert.Nil(t, err)
			assert.Equal(t, expectedStatus, status)
		})
		t.Run("should return all dag run status when total entries is greater than page limit request", func(t *testing.T) {
			respStringFirst := `{
    "dag_runs": [
        {
            "conf": {},
            "dag_id": "sample_select",
            "dag_run_id": "scheduled__2020-03-25T02:00:00+00:00",
            "end_date": "2020-06-01T17:32:58.489042+00:00",
            "execution_date": "2020-03-25T02:00:00+00:00",
            "external_trigger": false,
            "start_date": "2020-06-01T16:32:58.489042+00:00",
            "state": "success"
        },
        {
            "conf": {},
            "dag_id": "sample_select",
            "dag_run_id": "scheduled__2020-03-26T02:00:00+00:00",
            "end_date": "2020-06-01T16:33:01.020645+00:00",
            "execution_date": "2020-03-26T02:00:00+00:00",
            "external_trigger": false,
            "start_date": "2020-06-01T16:33:01.020645+00:00",
            "state": "failed"
        }
    ],
    "total_entries": 3
}`
			respStringSecond := `{
    "dag_runs": [
        {
            "conf": {},
            "dag_id": "sample_select",
            "dag_run_id": "scheduled__2020-03-27T02:00:00+00:00",
            "end_date": "2020-06-01T16:34:01.020645+00:00",
            "execution_date": "2020-03-26T02:00:00+00:00",
            "external_trigger": false,
            "start_date": "2020-06-01T16:35:01.020645+00:00",
            "state": "running"
        }
    ],
    "total_entries": 3
}`
			expectedExecutionTime0, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-25T02:00:00+00:00")
			expectedExecutionTime1, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-26T02:00:00+00:00")
			expectedExecutionTime2, _ := time.Parse(models.InstanceScheduledAtTimeLayout, "2020-03-26T02:00:00+00:00")
			expectedStatus := []models.JobStatus{
				{
					ScheduledAt: expectedExecutionTime0,
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: expectedExecutionTime1,
					State:       models.RunStateFailed,
				},
				{
					ScheduledAt: expectedExecutionTime2,
					State:       models.RunStateRunning,
				},
			}

			dagRunResp := []io.ReadCloser{
				ioutil.NopCloser(bytes.NewReader([]byte(respStringFirst))),
				ioutil.NopCloser(bytes.NewReader([]byte(respStringSecond))),
			}
			countDagRunReq := -1
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					countDagRunReq++
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       dagRunResp[countDagRunReq],
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			status, err := air.GetJobRunStatus(ctx, projectSpec, jobName, startDateTime, endDateTime, batchSize)

			assert.Nil(t, err)
			assert.Equal(t, expectedStatus, status)
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

			air := airflow2.NewScheduler(nil, client, nil)
			status, err := air.GetJobRunStatus(ctx, projectSpec, jobName, startDateTime, endDateTime, batchSize)

			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "failure reason for fetching airflow dag runs")
			assert.Len(t, status, 0)
		})
	})

	t.Run("GetJobRuns", func(t *testing.T) {
		host := "http://airflow.example.io"
		date, err := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		params := models.JobQuery{
			Name:      "sample_select",
			StartDate: date,
			EndDate:   date.Add(time.Hour * 24),
			Filter:    nil,
		}
		run := airflow2.DagRun{
			DagRunID:        "scheduled__2022-03-25T02:00:00+00:00",
			DagID:           "sample_select",
			LogicalDate:     date,
			ExecutionDate:   date,
			StartDate:       date.Add(time.Hour * 1),
			EndDate:         date.Add(time.Hour * 2),
			State:           "success",
			ExternalTrigger: false,
			Conf:            struct{}{},
		}
		list := airflow2.DagRunListResponse{
			DagRuns:      []airflow2.DagRun{run, run},
			TotalEntries: 2,
		}
		invalidList := airflow2.DagRunListResponse{
			DagRuns:      []airflow2.DagRun{run, run},
			TotalEntries: 100000,
		}
		resp, err := json.Marshal(list)
		if err != nil {
			t.Errorf("unable to parse the response to test GetJobRuns %v", err)
		}
		invalidResp, err := json.Marshal(invalidList)
		if err != nil {
			t.Errorf("unable to parse the invalid response to test GetJobRuns %v", err)
		}
		sch, err := cron.ParseCronSchedule("0 12 * * *")
		if err != nil {
			t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
		}

		t.Run("should return job runs with valid args", func(t *testing.T) {
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader(resp))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
					models.ProjectSchedulerAuth: "admin:admin",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.Nil(t, err)
			assert.Len(t, runStatus, 2)
		})
		t.Run("should fail if response has more entries than limit", func(t *testing.T) {
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader(invalidResp))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
					models.ProjectSchedulerAuth: "admin:admin",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.NotNil(t, err)
			assert.Nil(t, runStatus)
		})
		t.Run("should return job runs when LastRunOnly is true", func(t *testing.T) {
			// create a new reader with JSON
			r := ioutil.NopCloser(bytes.NewReader(resp))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}
			params.OnlyLastRun = true
			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
					models.ProjectSchedulerAuth: "admin:admin",
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.Nil(t, err)
			assert.Len(t, runStatus, 2)
		})
		t.Run("should fail if host fails to return OK", func(t *testing.T) {
			respString := "INTERNAL ERROR"
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.NotNil(t, err)
			assert.Len(t, runStatus, 0)
		})

		t.Run("should fail if host faulty json response", func(t *testing.T) {
			respString := "INTERNAL ERROR"
			r := ioutil.NopCloser(bytes.NewReader([]byte(respString)))
			client := &MockHTTPClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       r,
					}, nil
				},
			}

			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.NotNil(t, err)
			assert.Len(t, runStatus, 0)
		})
		t.Run("should fail if host is not reachable", func(t *testing.T) {
			client := &http.Client{}
			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
				Secret: []models.ProjectSecretItem{
					{
						Name:  models.ProjectSchedulerAuth,
						Value: "admin:admin",
					},
				},
			}, &params, sch)

			assert.NotNil(t, err)
			assert.Len(t, runStatus, 0)
		})
		t.Run("should fail if host is not configured", func(t *testing.T) {
			client := &http.Client{}
			air := airflow2.NewScheduler(nil, client, nil)
			runStatus, err := air.GetJobRuns(ctx, models.ProjectSpec{}, &params, sch)
			assert.NotNil(t, err)
			assert.Len(t, runStatus, 0)
		})
		t.Run("should fail if not scheduler secret registered", func(t *testing.T) {
			air := airflow2.NewScheduler(nil, nil, nil)
			_, err := air.GetJobRuns(ctx, models.ProjectSpec{
				Name: "test-proj",
				Config: map[string]string{
					models.ProjectSchedulerHost: host,
				},
			}, &params, sch)
			assert.NotNil(t, err)
		})
	})
}
