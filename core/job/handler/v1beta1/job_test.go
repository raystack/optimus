package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/handler/v1beta1"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func TestNewJobHandler(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate, "").Build()
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobTaskConfig)
	jobBehavior := &pb.JobSpecification_Behavior{
		Retry: &pb.JobSpecification_Behavior_Retry{ExponentialBackoff: false},
		Notify: []*pb.JobSpecification_Behavior_Notifiers{
			{On: 0, Channels: []string{"sample"}},
		},
	}
	jobDependencies := []*pb.JobDependency{
		{Name: "job-B", Type: "static"},
	}
	jobMetadata := &pb.JobMetadata{
		Resource: &pb.JobSpecMetadataResource{
			Request: &pb.JobSpecMetadataResourceConfig{Cpu: "1", Memory: "8"},
			Limit:   &pb.JobSpecMetadataResourceConfig{Cpu: ".5", Memory: "4"},
		},
		Airflow: &pb.JobSpecMetadataAirflow{Pool: "100", Queue: "50"},
	}

	t.Run("AddJobSpecifications", func(t *testing.T) {
		t.Run("adds job", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				StartDate:        jobSchedule.StartDate().String(),
				EndDate:          jobSchedule.EndDate().String(),
				Interval:         jobSchedule.Interval(),
				TaskName:         jobTask.Name().String(),
				WindowSize:       jobWindow.GetSize(),
				WindowOffset:     jobWindow.GetOffset(),
				WindowTruncateTo: jobWindow.GetTruncateTo(),
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil, nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log: "jobs are created and queued for deployment on project test-proj with error: 1 error occurred:\n\t* invalid argument for entity job: owner is empty\n\n",
			}, resp)
		})
		t.Run("adds complete job and returns deployment ID", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				StartDate:        jobSchedule.StartDate().String(),
				EndDate:          jobSchedule.EndDate().String(),
				Interval:         jobSchedule.Interval(),
				TaskName:         jobTask.Name().String(),
				WindowSize:       jobWindow.GetSize(),
				WindowOffset:     jobWindow.GetOffset(),
				WindowTruncateTo: jobWindow.GetTruncateTo(),
				Behavior:         jobBehavior,
				Dependencies:     jobDependencies,
				Metadata:         jobMetadata,
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil, nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log: "jobs are created and queued for deployment on project test-proj with error: 1 error occurred:\n\t* invalid argument for entity job: owner is empty\n\n",
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			request := pb.AddJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:          int32(0),
					Name:             "job-A",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
				{
					Version:          int32(jobVersion),
					Name:             "job-B",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
			}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil, nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.NotNil(t, resp.DeploymentId)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns error when unable to do Add", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:          int32(0),
					Name:             "job-A",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
			}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil, errors.New("internal error"))

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns response with job errors log when some jobs failed to be added", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:          int32(jobVersion),
					Name:             "job-A",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
				{
					Version:          int32(jobVersion),
					Name:             "job-B",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
			}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(errors.New("some jobs failed to be added"), nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
	})
}

// JobService is an autogenerated mock type for the JobService type
type JobService struct {
	mock.Mock
}

// Add provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) (error, error) {
	ret := _m.Called(ctx, jobTenant, jobs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		r0 = ret.Error(0)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r1 = rf(ctx, jobTenant, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
