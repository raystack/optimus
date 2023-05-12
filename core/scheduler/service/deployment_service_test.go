package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

func TestDeploymentService(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	proj1Name := tenant.ProjectName("proj1")
	namespace1Name := tenant.ProjectName("ns1")
	namespace2Name := tenant.ProjectName("ns2")
	tnnt1, _ := tenant.NewTenant(proj1Name.String(), namespace1Name.String())
	tnnt2, _ := tenant.NewTenant(proj1Name.String(), namespace2Name.String())

	jobUpstreamStatic := scheduler.JobUpstream{
		JobName:        "job3",
		Host:           "some-host",
		TaskName:       "bq2bq",
		DestinationURN: "bigquery://some-resource",
		Tenant:         tenant.Tenant{},
		Type:           scheduler.UpstreamTypeStatic,
		External:       true,
		State:          "resolved",
	}
	jobUpstreamInferred := scheduler.JobUpstream{
		JobName:        "job3",
		Host:           "some-host",
		TaskName:       "bq2bq",
		DestinationURN: "bigquery://some-resource",
		Tenant:         tenant.Tenant{},
		Type:           scheduler.UpstreamTypeInferred,
		External:       true,
		State:          "resolved",
	}
	jobsWithDetails := []*scheduler.JobWithDetails{
		{
			Name: "job1",
			Job: &scheduler.Job{
				Name:   "job1",
				Tenant: tnnt1,
			},
		},
		{
			Name: "job2",
			Job: &scheduler.Job{
				Name:   "job2",
				Tenant: tnnt2,
			},
		},
		{
			Name: "job3",
			Job: &scheduler.Job{
				Name:   "job3",
				Tenant: tnnt1,
			},
			Upstreams: scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{&jobUpstreamStatic, &jobUpstreamInferred},
			},
		},
	}

	t.Run("UploadToScheduler", func(t *testing.T) {
		t.Run("should return error if unable to get all jobs from job repo", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetAll", mock.Anything, proj1Name).Return(nil, fmt.Errorf("GetAll error"))
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, nil, nil, nil, nil, nil, nil, nil)

			err := runService.UploadToScheduler(ctx, proj1Name)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errorInUploadToScheduler:\n GetAll error")
		})
		t.Run("should return error if error in priority resolution", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetAll", mock.Anything, proj1Name).Return(jobsWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			priorityResolver := new(mockPriorityResolver)
			priorityResolver.On("Resolve", mock.Anything, jobsWithDetails).Return(fmt.Errorf("priority resolution error"))
			defer priorityResolver.AssertExpectations(t)

			runService := service.NewJobRunService(nil,
				jobRepo, nil, nil, nil, nil, priorityResolver, nil, nil)

			err := runService.UploadToScheduler(ctx, proj1Name)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errorInUploadToScheduler:\n priority resolution error")
		})
		t.Run("should deploy Jobs Per Namespace returning error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetAll", mock.Anything, proj1Name).Return([]*scheduler.JobWithDetails{jobsWithDetails[0], jobsWithDetails[2]}, nil)
			defer jobRepo.AssertExpectations(t)

			priorityResolver := new(mockPriorityResolver)
			priorityResolver.On("Resolve", mock.Anything, []*scheduler.JobWithDetails{jobsWithDetails[0], jobsWithDetails[2]}).Return(nil)
			defer priorityResolver.AssertExpectations(t)

			mScheduler := new(mockScheduler)
			mScheduler.On("DeployJobs", mock.Anything, tnnt1, []*scheduler.JobWithDetails{jobsWithDetails[0], jobsWithDetails[2]}).
				Return(fmt.Errorf("DeployJobs tnnt1 error"))
			defer mScheduler.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil,
				mScheduler, priorityResolver, nil, nil)

			err := runService.UploadToScheduler(ctx, proj1Name)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errorInUploadToScheduler:\n DeployJobs tnnt1 error")
		})
		t.Run("should deploy Jobs Per Namespace and cleanPerNamespace, appropriately", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetAll", mock.Anything, proj1Name).Return(jobsWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			priorityResolver := new(mockPriorityResolver)
			priorityResolver.On("Resolve", mock.Anything, jobsWithDetails).Return(nil)
			defer priorityResolver.AssertExpectations(t)

			mScheduler := new(mockScheduler)
			mScheduler.On("DeployJobs", mock.Anything, tnnt1, []*scheduler.JobWithDetails{jobsWithDetails[0], jobsWithDetails[2]}).
				Return(nil)
			mScheduler.On("DeployJobs", mock.Anything, tnnt2, []*scheduler.JobWithDetails{jobsWithDetails[1]}).
				Return(nil)
			mScheduler.On("ListJobs", mock.Anything, tnnt1).Return([]string{"job1", "job3"}, nil)
			mScheduler.On("ListJobs", mock.Anything, tnnt2).Return([]string{"job2", "job4-to-delete"}, nil)
			var jobsToDelete []string
			mScheduler.On("DeleteJobs", mock.Anything, tnnt1, jobsToDelete).Return(nil)
			mScheduler.On("DeleteJobs", mock.Anything, tnnt2, []string{"job4-to-delete"}).Return(nil)
			defer mScheduler.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil,
				mScheduler, priorityResolver, nil, nil)

			err := runService.UploadToScheduler(ctx, proj1Name)
			assert.Nil(t, err)
		})
		t.Run("should deploy Jobs Per Namespace and cleanPerNamespace, appropriately", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetAll", mock.Anything, proj1Name).Return(jobsWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			priorityResolver := new(mockPriorityResolver)
			priorityResolver.On("Resolve", mock.Anything, jobsWithDetails).Return(nil)
			defer priorityResolver.AssertExpectations(t)

			mScheduler := new(mockScheduler)
			mScheduler.On("DeployJobs", mock.Anything, tnnt1, []*scheduler.JobWithDetails{jobsWithDetails[0], jobsWithDetails[2]}).
				Return(nil)
			mScheduler.On("DeployJobs", mock.Anything, tnnt2, []*scheduler.JobWithDetails{jobsWithDetails[1]}).
				Return(nil)
			mScheduler.On("ListJobs", mock.Anything, tnnt1).Return([]string{}, fmt.Errorf("listJobs error"))
			mScheduler.On("ListJobs", mock.Anything, tnnt2).Return([]string{"job2", "job4-to-delete"}, nil)
			mScheduler.On("DeleteJobs", mock.Anything, tnnt2, []string{"job4-to-delete"}).Return(nil)
			defer mScheduler.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil,
				mScheduler, priorityResolver, nil, nil)

			err := runService.UploadToScheduler(ctx, proj1Name)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errorInUploadToScheduler:\n listJobs error")
		})
	})
}

type mockPriorityResolver struct {
	mock.Mock
}

func (m *mockPriorityResolver) Resolve(ctx context.Context, details []*scheduler.JobWithDetails) error {
	args := m.Called(ctx, details)
	return args.Error(0)
}
