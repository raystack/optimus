package v1beta1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
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
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()
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

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.ErrorContains(t, err, "no jobs to be processed")
			assert.Nil(t, resp)
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
			assert.ErrorContains(t, err, "no jobs to be processed")
			assert.Nil(t, resp)
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
	t.Run("ReplaceAllJobSpecifications", func(t *testing.T) {
		var jobNamesToSkip []job.Name
		t.Run("replaces all job specifications of a tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobProtos := []*pb.JobSpecification{
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
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesToSkip, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Nil(t, err)
		})
		t.Run("replaces all job specifications given multiple tenant", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobProtos := []*pb.JobSpecification{
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
			}
			request1 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			otherTenant, _ := tenant.NewTenant(project.Name().String(), "other-namespace")
			request2 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: otherTenant.NamespaceName().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request1, nil).Once()
			stream.On("Recv").Return(request2, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesToSkip, mock.Anything).Return(nil)
			jobService.On("ReplaceAll", ctx, otherTenant, mock.Anything, jobNamesToSkip, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Nil(t, err)
		})
		t.Run("skips a job if the proto is invalid", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version: int32(jobVersion),
					Name:    "job-A",
					Owner:   "sample-owner",
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
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, []job.Name{"job-A"}, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.ErrorContains(t, err, "error when replacing job specifications")
		})
		t.Run("skips operation for a namespace if the tenant is invalid", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobProtos := []*pb.JobSpecification{
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
			request1 := &pb.ReplaceAllJobSpecificationsRequest{}
			request2 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request1, nil).Once()
			stream.On("Recv").Return(request2, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesToSkip, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Times(3)

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Error(t, err)
		})
		t.Run("marks operation for this namespace to failed if unable to successfully do replace all", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			jobProtos := []*pb.JobSpecification{
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
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesToSkip, mock.Anything).Return(errors.New("internal error"))

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Error(t, err)
		})
	})
	t.Run("RefreshJobs", func(t *testing.T) {
		t.Run("do refresh for the requested jobs", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			request := &pb.RefreshJobsRequest{
				ProjectName:    project.Name().String(),
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			jobService.On("Refresh", ctx, project.Name(), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.Nil(t, err)
		})
		t.Run("returns error if project name is invalid", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			request := &pb.RefreshJobsRequest{
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.Error(t, err)
		})
		t.Run("returns error if unable to successfully run refresh", func(t *testing.T) {
			jobService := new(JobService)

			jobHandler := v1beta1.NewJobHandler(jobService, log)

			request := &pb.RefreshJobsRequest{
				ProjectName:    project.Name().String(),
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			jobService.On("Refresh", ctx, project.Name(), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("internal error"))

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.ErrorContains(t, err, "internal error")
		})
	})
	t.Run("GetJobSpecification", func(t *testing.T) {
		t.Run("return error when tenant creation failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when job name is empty", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when service get failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       "job-A",
			}

			jobService.On("Get", ctx, sampleTenant, job.Name("job-A")).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})
	})
	t.Run("GetJobSpecifications", func(t *testing.T) {
		t.Run("return error when service get by filter is failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationsRequest{}

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecifications(ctx, &request)
			assert.Error(t, err)
			assert.NotNil(t, resp)
			assert.Empty(t, resp.JobSpecificationResponses)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationsRequest{}

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "table-B", []job.ResourceURN{"table-C"})

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.GetJobSpecifications(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.JobSpecificationResponses)
			assert.Len(t, resp.JobSpecificationResponses, 2)
		})
	})
	t.Run("ListJobSpecification", func(t *testing.T) {
		t.Run("return error when service get by filter failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.ListJobSpecificationRequest{}

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.ListJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.NotNil(t, resp)
			assert.Empty(t, resp.Jobs)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			request := pb.ListJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "table-B", []job.ResourceURN{"table-C"})

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, log)
			resp, err := jobHandler.ListJobSpecification(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.Jobs)
			assert.Len(t, resp.Jobs, 2)
		})
	})
	t.Run("CheckJobSpecifications", func(t *testing.T) {
		t.Run("return error when creating tenant failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			stream := new(CheckJobSpecificationsServer)
			defer stream.AssertExpectations(t)

			request := &pb.CheckJobSpecificationsRequest{}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			err := jobHandler.CheckJobSpecifications(request, stream)
			assert.Error(t, err)
			assert.Equal(t, "invalid argument for entity project: project name is empty", err.Error())
		})
		t.Run("return error when job proto conversion failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			stream := new(CheckJobSpecificationsServer)
			defer stream.AssertExpectations(t)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "",
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

			stream.On("Context").Return(ctx)
			jobService.On("Validate", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)

			request := &pb.CheckJobSpecificationsRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				Jobs:          jobProtos,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			err := jobHandler.CheckJobSpecifications(request, stream)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid argument for entity job: name is empty")
		})
		t.Run("return error when service validate job is failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			stream := new(CheckJobSpecificationsServer)
			defer stream.AssertExpectations(t)

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

			stream.On("Context").Return(ctx)
			jobService.On("Validate", ctx, sampleTenant, mock.Anything, mock.Anything).Return(errors.New("error encountered"))

			request := &pb.CheckJobSpecificationsRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				Jobs:          jobProtos,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			err := jobHandler.CheckJobSpecifications(request, stream)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error encountered")
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			stream := new(CheckJobSpecificationsServer)
			defer stream.AssertExpectations(t)

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

			stream.On("Context").Return(ctx)
			jobService.On("Validate", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)

			request := &pb.CheckJobSpecificationsRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				Jobs:          jobProtos,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, log)
			err := jobHandler.CheckJobSpecifications(request, stream)
			assert.NoError(t, err)
		})
	})
	t.Run("JobInspect", func(t *testing.T) {
		t.Run("should return basic info, upstream, downstream of an existing job", func(t *testing.T) {
			jobService := new(JobService)

			httpUpstream := job.NewSpecHTTPUpstreamBuilder("sample-upstream", "sample-url").Build()
			upstreamSpec := job.NewSpecUpstreamBuilder().WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", "resource-b", sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", "resource-c", sampleTenant, "inferred", "bq2bq", true)

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
			}
			jobADownstream := []*dto.Downstream{
				{
					Name:          "job-x",
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					TaskName:      jobTask.Name().String(),
				},
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(jobA, basicInfoLogger)
			jobService.On("GetUpstreamsToInspect", ctx, jobA, false).Return(jobAUpstream, nil)
			jobService.On("GetDownstream", ctx, jobA, false).Return(jobADownstream, nil)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:          int32(jobVersion),
						Name:             specA.Name().String(),
						StartDate:        specA.Schedule().StartDate().String(),
						EndDate:          specA.Schedule().EndDate().String(),
						Interval:         specA.Schedule().Interval(),
						DependsOnPast:    specA.Schedule().DependsOnPast(),
						CatchUp:          specA.Schedule().CatchUp(),
						TaskName:         specA.Task().Name().String(),
						WindowSize:       specA.Window().GetSize(),
						WindowOffset:     specA.Window().GetOffset(),
						WindowTruncateTo: specA.Window().GetTruncateTo(),
						Destination:      "resource-A",
						Config: []*pb.JobConfigItem{{
							Name:  "sample_task_key",
							Value: "sample_value",
						}},
						Dependencies: []*pb.JobDependency{
							{
								HttpDependency: &pb.HttpDependency{
									Name: httpUpstream.Name().String(),
									Url:  httpUpstream.URL(),
								},
							},
						},
					},
					Destination: "resource-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
					HttpDependency: []*pb.HttpDependency{
						{
							Name: httpUpstream.Name().String(),
							Url:  httpUpstream.URL(),
						},
					},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name,
							ProjectName:   jobADownstream[0].ProjectName,
							NamespaceName: jobADownstream[0].NamespaceName,
							TaskName:      jobADownstream[0].TaskName,
						},
					},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return basic info, upstream, downstream of a user given job spec", func(t *testing.T) {
			jobService := new(JobService)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", "resource-b", sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", "resource-c", sampleTenant, "inferred", "bq2bq", true)

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
			}
			jobADownstream := []*dto.Downstream{
				{
					Name:          "job-x",
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					TaskName:      jobTask.Name().String(),
				},
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, mock.Anything, mock.Anything).Return(jobA, basicInfoLogger)
			jobService.On("GetUpstreamsToInspect", ctx, jobA, true).Return(jobAUpstream, nil)
			jobService.On("GetDownstream", ctx, jobA, true).Return(jobADownstream, nil)

			jobSpecProto := &pb.JobSpecification{
				Version:          int32(jobVersion),
				Name:             "job-A",
				Owner:            "sample-owner",
				StartDate:        jobSchedule.StartDate().String(),
				Interval:         jobSchedule.Interval(),
				TaskName:         jobTask.Name().String(),
				WindowSize:       jobWindow.GetSize(),
				WindowOffset:     jobWindow.GetOffset(),
				WindowTruncateTo: jobWindow.GetTruncateTo(),
				Behavior:         jobBehavior,
				Dependencies:     jobDependencies,
				Metadata:         jobMetadata,
			}
			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Spec:          jobSpecProto,
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:          int32(jobVersion),
						Name:             specA.Name().String(),
						Owner:            "sample-owner",
						StartDate:        specA.Schedule().StartDate().String(),
						EndDate:          specA.Schedule().EndDate().String(),
						Interval:         specA.Schedule().Interval(),
						DependsOnPast:    specA.Schedule().DependsOnPast(),
						CatchUp:          specA.Schedule().CatchUp(),
						TaskName:         specA.Task().Name().String(),
						WindowSize:       specA.Window().GetSize(),
						WindowOffset:     specA.Window().GetOffset(),
						WindowTruncateTo: specA.Window().GetTruncateTo(),
						Destination:      "resource-A",
						Config: []*pb.JobConfigItem{{
							Name:  "sample_task_key",
							Value: "sample_value",
						}},
					},
					Destination: "resource-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name,
							ProjectName:   jobADownstream[0].ProjectName,
							NamespaceName: jobADownstream[0].NamespaceName,
							TaskName:      jobADownstream[0].TaskName,
						},
					},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return error if tenant is invalid", func(t *testing.T) {
			jobService := new(JobService)

			req := &pb.JobInspectRequest{
				ProjectName: project.Name().String(),
				JobName:     "job-A",
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return error if job name and spec are not provided", func(t *testing.T) {
			jobService := new(JobService)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return error if job spec proto is invalid", func(t *testing.T) {
			jobService := new(JobService)

			jobSpecProto := &pb.JobSpecification{
				Name: "job-A",
			}
			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Spec:          jobSpecProto,
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return downstream and upstream error log messages if exist", func(t *testing.T) {
			jobService := new(JobService)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", "resource-b", sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", "resource-c", sampleTenant, "inferred", "bq2bq", true)

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
			}
			jobADownstream := []*dto.Downstream{
				{
					Name:          "job-x",
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					TaskName:      jobTask.Name().String(),
				},
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(jobA, basicInfoLogger)

			upstreamErrorMsg := "sample upstream error"
			jobService.On("GetUpstreamsToInspect", ctx, jobA, false).Return(jobAUpstream, errors.New(upstreamErrorMsg))

			downstreamErrorMsg := "sample downstream error"
			jobService.On("GetDownstream", ctx, jobA, false).Return(jobADownstream, errors.New(downstreamErrorMsg))

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:          int32(jobVersion),
						Name:             specA.Name().String(),
						StartDate:        specA.Schedule().StartDate().String(),
						EndDate:          specA.Schedule().EndDate().String(),
						Interval:         specA.Schedule().Interval(),
						DependsOnPast:    specA.Schedule().DependsOnPast(),
						CatchUp:          specA.Schedule().CatchUp(),
						TaskName:         specA.Task().Name().String(),
						WindowSize:       specA.Window().GetSize(),
						WindowOffset:     specA.Window().GetOffset(),
						WindowTruncateTo: specA.Window().GetTruncateTo(),
						Destination:      "resource-A",
						Config: []*pb.JobConfigItem{{
							Name:  "sample_task_key",
							Value: "sample_value",
						}},
					},
					Destination: "resource-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
					Notice: []*pb.Log{{Level: pb.Level_LEVEL_ERROR, Message: "unable to get upstream jobs: sample upstream error"}},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name,
							ProjectName:   jobADownstream[0].ProjectName,
							NamespaceName: jobADownstream[0].NamespaceName,
							TaskName:      jobADownstream[0].TaskName,
						},
					},
					Notice: []*pb.Log{{Level: pb.Level_LEVEL_ERROR, Message: "unable to get downstream jobs: sample downstream error"}},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, nil)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return error if job basic info is not found", func(t *testing.T) {
			jobService := new(JobService)

			httpUpstream := job.NewSpecHTTPUpstreamBuilder("sample-upstream", "sample-url").Build()
			upstreamSpec := job.NewSpecUpstreamBuilder().WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()

			basicInfoLogger := writer.BufferedLogger{Messages: []*pb.Log{
				{Message: "not found"},
			}}
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(nil, basicInfoLogger)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Nil(t, result)
			assert.ErrorContains(t, err, "not found")
		})
	})
	t.Run("GetJobTask", func(t *testing.T) {
		t.Run("return error when create tenant failed", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{}

			handler := v1beta1.NewJobHandler(jobService, nil)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, "invalid argument for entity project: project name is empty", err.Error())
		})
		t.Run("return error when job name is empty", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, nil)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, "invalid argument for entity job: name is empty", err.Error())
		})
		t.Run("return error when service get job eror", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       "job-A",
			}

			jobService.On("Get", ctx, sampleTenant, job.Name("job-A")).Return(nil, errors.New("error encountered"))
			handler := v1beta1.NewJobHandler(jobService, nil)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when service get task info error", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobService.On("GetTaskInfo", ctx, jobA.Spec().Task()).Return(nil, errors.New("error encountered"))
			handler := v1beta1.NewJobHandler(jobService, nil)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			defer jobService.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			jobTask := job.NewTaskBuilder(jobTask.Name(), jobTask.Config()).WithInfo(&models.PluginInfoResponse{
				Name:        "bq2bq",
				Description: "task info desc",
				Image:       "odpf/bq2bq:latest",
			}).Build()
			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobService.On("GetTaskInfo", ctx, jobA.Spec().Task()).Return(jobTask, nil)
			handler := v1beta1.NewJobHandler(jobService, nil)
			resp, err := handler.GetJobTask(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp)
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

// Get provides a mock function with given fields: ctx, jobTenant, jobName
func (_m *JobService) Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (*job.Job, error) {
	ret := _m.Called(ctx, jobTenant, jobName)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name) *job.Job); ok {
		r0 = rf(ctx, jobTenant, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name) error); ok {
		r1 = rf(ctx, jobTenant, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAll provides a mock function with given fields: ctx, filters
func (_m *JobService) GetByFilter(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
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

// GetDownstream provides a mock function with given fields: ctx, _a1, localJob
func (_m *JobService) GetDownstream(ctx context.Context, _a1 *job.Job, localJob bool) ([]*dto.Downstream, error) {
	ret := _m.Called(ctx, _a1, localJob)

	var r0 []*dto.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job, bool) []*dto.Downstream); ok {
		r0 = rf(ctx, _a1, localJob)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*dto.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job, bool) error); ok {
		r1 = rf(ctx, _a1, localJob)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobBasicInfo provides a mock function with given fields: ctx, jobTenant, jobName, spec
func (_m *JobService) GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger) {
	ret := _m.Called(ctx, jobTenant, jobName, spec)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name, *job.Spec) *job.Job); ok {
		r0 = rf(ctx, jobTenant, jobName, spec)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 writer.BufferedLogger
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name, *job.Spec) writer.BufferedLogger); ok {
		r1 = rf(ctx, jobTenant, jobName, spec)
	} else {
		r1 = ret.Get(1).(writer.BufferedLogger)
	}

	return r0, r1
}

// GetTaskInfo provides a mock function with given fields: ctx, task
func (_m *JobService) GetTaskInfo(ctx context.Context, task *job.Task) (*job.Task, error) {
	ret := _m.Called(ctx, task)

	var r0 *job.Task
	if rf, ok := ret.Get(0).(func(context.Context, *job.Task) *job.Task); ok {
		r0 = rf(ctx, task)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Task)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Task) error); ok {
		r1 = rf(ctx, task)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetUpstreamsToInspect provides a mock function with given fields: ctx, subjectJob, localJob
func (_m *JobService) GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, subjectJob, localJob)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job, bool) []*job.Upstream); ok {
		r0 = rf(ctx, subjectJob, localJob)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job, bool) error); ok {
		r1 = rf(ctx, subjectJob, localJob)
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

// ReplaceAll provides a mock function with given fields: ctx, jobTenant, jobs, jobNamesToSkip, logWriter
func (_m *JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, jobNamesToSkip []job.Name, logWriter writer.LogWriter) error {
	ret := _m.Called(ctx, jobTenant, jobs, jobNamesToSkip, logWriter)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec, []job.Name, writer.LogWriter) error); ok {
		r0 = rf(ctx, jobTenant, jobs, jobNamesToSkip, logWriter)
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

// Validate provides a mock function with given fields: ctx, jobTenant, jobSpecs, logWriter
func (_m *JobService) Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, logWriter writer.LogWriter) error {
	ret := _m.Called(ctx, jobTenant, jobSpecs, logWriter)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec, writer.LogWriter) error); ok {
		r0 = rf(ctx, jobTenant, jobSpecs, logWriter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplaceAllJobSpecificationsServer is an autogenerated mock type for the ReplaceAllJobSpecificationsServer type
type ReplaceAllJobSpecificationsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *ReplaceAllJobSpecificationsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// Recv provides a mock function with given fields:
func (_m *ReplaceAllJobSpecificationsServer) Recv() (*pb.ReplaceAllJobSpecificationsRequest, error) {
	ret := _m.Called()

	var r0 *pb.ReplaceAllJobSpecificationsRequest
	if rf, ok := ret.Get(0).(func() *pb.ReplaceAllJobSpecificationsRequest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pb.ReplaceAllJobSpecificationsRequest)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RecvMsg provides a mock function with given fields: m
func (_m *ReplaceAllJobSpecificationsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) Send(_a0 *pb.ReplaceAllJobSpecificationsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.ReplaceAllJobSpecificationsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *ReplaceAllJobSpecificationsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

// RefreshJobsServer is an autogenerated mock type for the RefreshJobsServer type
type RefreshJobsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *RefreshJobsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// RecvMsg provides a mock function with given fields: m
func (_m *RefreshJobsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) Send(_a0 *pb.RefreshJobsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.RefreshJobsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *RefreshJobsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

// CheckJobSpecificationsServer is an autogenerated mock type for the CheckJobSpecificationsServer type
type CheckJobSpecificationsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *CheckJobSpecificationsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// RecvMsg provides a mock function with given fields: m
func (_m *CheckJobSpecificationsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) Send(_a0 *pb.CheckJobSpecificationsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.CheckJobSpecificationsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *CheckJobSpecificationsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}
