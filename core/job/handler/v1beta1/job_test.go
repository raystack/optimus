package v1beta1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/handler/v1beta1"
	"github.com/odpf/optimus/core/job/service/filter"
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
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
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
	log := log.NewNoop()

	t.Run("AddJobSpecifications", func(t *testing.T) {
		t.Run("adds job", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				Owner:            "sample-owner",
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

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log: "jobs are successfully created",
			}, resp)
		})
		t.Run("adds complete job", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				Owner:            "sample-owner",
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

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log: "jobs are successfully created",
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			request := pb.AddJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

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
					Owner:            "sample-owner",
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

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns error when all jobs failed to be added", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

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

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(errors.New("internal error"))

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns response with job errors log when some jobs failed to be added", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:          int32(jobVersion),
					Name:             "job-A",
					Owner:            "sample-owner",
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

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(errors.New("internal error"))

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
	})
	t.Run("UpdateJobSpecifications", func(t *testing.T) {
		t.Run("update jobs", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				Owner:            "sample-owner",
				StartDate:        jobSchedule.StartDate().String(),
				EndDate:          jobSchedule.EndDate().String(),
				Interval:         jobSchedule.Interval(),
				TaskName:         jobTask.Name().String(),
				WindowSize:       jobWindow.GetSize(),
				WindowOffset:     jobWindow.GetOffset(),
				WindowTruncateTo: jobWindow.GetTruncateTo(),
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateJobSpecificationsResponse{
				Log: "jobs are successfully updated",
			}, resp)
		})
		t.Run("update complete jobs", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				Owner:            "sample-owner",
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
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateJobSpecificationsResponse{
				Log: "jobs are successfully updated",
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			request := pb.UpdateJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

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
					Owner:            "sample-owner",
					StartDate:        jobSchedule.StartDate().String(),
					EndDate:          jobSchedule.EndDate().String(),
					Interval:         jobSchedule.Interval(),
					TaskName:         jobTask.Name().String(),
					WindowSize:       jobWindow.GetSize(),
					WindowOffset:     jobWindow.GetOffset(),
					WindowTruncateTo: jobWindow.GetTruncateTo(),
				},
			}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return(nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns error when all jobs failed to be updated", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

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
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return(errors.New("internal error"))

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns response with job errors log when some jobs failed to be updated", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:          int32(jobVersion),
					Name:             "job-A",
					Owner:            "sample-owner",
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
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return(errors.New("internal error"))

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
	})
	t.Run("DeleteJobSpecification", func(t *testing.T) {
		t.Run("deletes job successfully", func(t *testing.T) {
			jobService := new(JobService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         false,
			}

			jobService.On("Delete", ctx, sampleTenant, jobAName, false, false).Return(nil, nil)

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.NoError(t, err)
			assert.NotContains(t, resp.Message, "these downstream will be affected")
		})
		t.Run("force deletes job with downstream successfully", func(t *testing.T) {
			jobService := new(JobService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			downstreamNames := []job.FullName{"job-B"}
			jobService.On("Delete", ctx, sampleTenant, jobAName, false, true).Return(downstreamNames, nil)

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.NoError(t, err)
			assert.Contains(t, resp.Message, fmt.Sprintf("these downstream will be affected: %s", downstreamNames))
		})
		t.Run("returns error if unable to construct tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if job name is not found", func(t *testing.T) {
			jobService := new(JobService)

			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if unable to delete job", func(t *testing.T) {
			jobService := new(JobService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobService.On("Delete", ctx, sampleTenant, jobAName, false, true).Return(nil, errors.New("internal error"))

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
	})
	t.Run("GetWindow", func(t *testing.T) {
		t.Run("returns error if sheduleAt is not valid", func(t *testing.T) {
			req := &pb.GetWindowRequest{
				ScheduledAt: nil,
			}
			jobHandler := v1beta1.NewJobHandler(nil, nil)

			resp, err := jobHandler.GetWindow(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if version is not valid", func(t *testing.T) {
			req := &pb.GetWindowRequest{
				Version:     3,
				ScheduledAt: timestamppb.New(time.Date(2022, 11, 18, 13, 0, 0, 0, time.UTC)),
			}
			jobHandler := v1beta1.NewJobHandler(nil, nil)

			resp, err := jobHandler.GetWindow(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if window is not valid", func(t *testing.T) {
			req := &pb.GetWindowRequest{
				Version:     2,
				ScheduledAt: timestamppb.New(time.Date(2022, 11, 18, 13, 0, 0, 0, time.UTC)),
				Size:        "1",
			}
			jobHandler := v1beta1.NewJobHandler(nil, nil)

			resp, err := jobHandler.GetWindow(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns dstart and dend", func(t *testing.T) {
			req := &pb.GetWindowRequest{
				Version:     2,
				ScheduledAt: timestamppb.New(time.Date(2022, 11, 18, 13, 0, 0, 0, time.UTC)),
				Size:        "24h",
				Offset:      "0",
				TruncateTo:  "d",
			}
			jobHandler := v1beta1.NewJobHandler(nil, nil)

			resp, err := jobHandler.GetWindow(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})
	})
}

// JobService is an autogenerated mock type for the JobService type
type JobService struct {
	mock.Mock
}

// Add provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error {
	ret := _m.Called(ctx, jobTenant, jobs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, jobTenant, jobName, cleanFlag, forceFlag
func (_m *JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag bool, forceFlag bool) ([]job.FullName, error) {
	ret := _m.Called(ctx, jobTenant, jobName, cleanFlag, forceFlag)

	var r0 []job.FullName
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name, bool, bool) []job.FullName); ok {
		r0 = rf(ctx, jobTenant, jobName, cleanFlag, forceFlag)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.FullName)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name, bool, bool) error); ok {
		r1 = rf(ctx, jobTenant, jobName, cleanFlag, forceFlag)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: ctx, filters
func (_m *JobService) Get(ctx context.Context, filters ...filter.FilterOpt) (*job.Job, error) {
	_va := make([]interface{}, len(filters))
	for _i := range filters {
		_va[_i] = filters[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, ...filter.FilterOpt) *job.Job); ok {
		r0 = rf(ctx, filters...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...filter.FilterOpt) error); ok {
		r1 = rf(ctx, filters...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAll provides a mock function with given fields: ctx, filters
func (_m *JobService) GetAll(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
	_va := make([]interface{}, len(filters))
	for _i := range filters {
		_va[_i] = filters[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, ...filter.FilterOpt) []*job.Job); ok {
		r0 = rf(ctx, filters...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...filter.FilterOpt) error); ok {
		r1 = rf(ctx, filters...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Refresh provides a mock function with given fields: ctx, projectName, logWriter, filters
func (_m *JobService) Refresh(ctx context.Context, projectName tenant.ProjectName, logWriter writer.LogWriter, filters ...filter.FilterOpt) error {
	_va := make([]interface{}, len(filters))
	for _i := range filters {
		_va[_i] = filters[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, projectName, logWriter)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, writer.LogWriter, ...filter.FilterOpt) error); ok {
		r0 = rf(ctx, projectName, logWriter, filters...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplaceAll provides a mock function with given fields: ctx, jobTenant, jobs, logWriter
func (_m *JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, logWriter writer.LogWriter) error {
	ret := _m.Called(ctx, jobTenant, jobs, logWriter)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec, writer.LogWriter) error); ok {
		r0 = rf(ctx, jobTenant, jobs, logWriter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Update(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) error {
	ret := _m.Called(ctx, jobTenant, jobs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
