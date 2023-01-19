package resolver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/resolver"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/resourcemanager"
	"github.com/odpf/optimus/internal/models"
)

func TestExternalUpstreamResolver(t *testing.T) {
	ctx := context.Background()
	sampleTenant, _ := tenant.NewTenant("project", "namespace")
	externalTenant, _ := tenant.NewTenant("external-project", "external-namespace")
	resourceManager := new(ResourceManager)
	optimusResourceManagers := []resourcemanager.ResourceManager{resourceManager}

	jobVersion := 1
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTaskConfig, _ := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTaskBuilder(taskName, jobTaskConfig).Build()
	upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"external-project/job-B"}).Build()
	specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
	jobA := job.NewJob(sampleTenant, specA, "", []job.ResourceURN{"resource-C"})

	t.Run("BulkResolve", func(t *testing.T) {
		t.Run("resolves upstream externally", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred("resource-C")
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			upstreamB := job.NewUpstreamResolved("job-B", "external-host", "resource-B", externalTenant, "static", taskName, true)
			upstreamC := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "inferred", taskName, true)
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{upstreamB}, nil).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{upstreamC}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, result[0].GetUnresolvedUpstreams())
			assert.Nil(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB, upstreamC}, result[0].Upstreams())
		})
		t.Run("returns the merged of previous resolved and external resolved upstreams", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred("resource-C")
			upstreamD := job.NewUpstreamResolved("job-D", "internal-host", "resource-D", sampleTenant, "inferred", taskName, false)
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, upstreamD})

			upstreamB := job.NewUpstreamResolved("job-B", "external-host", "resource-B", externalTenant, "static", taskName, true)
			upstreamC := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "inferred", taskName, true)
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{upstreamB}, nil).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{upstreamC}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, result[0].GetUnresolvedUpstreams())
			assert.Nil(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamD, upstreamB, upstreamC}, result[0].Upstreams())
		})
		t.Run("returns unresolved upstream and upstream error if unable to fetch upstreams from external", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred("resource-C")
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{}, errors.New("connection error")).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.EqualValues(t, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}, result[0].Upstreams())
			assert.NotNil(t, err)
		})
		t.Run("skips resolves upstream externally if no external resource manager found", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred("resource-C")
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(nil)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, err)
			assert.EqualValues(t, jobWithUnresolvedUpstream, result[0])
		})
	})
	t.Run("NewExternalUpstreamResolver", func(t *testing.T) {
		t.Run("should able to construct external upstream resolver using resource manager config", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "optimus",
				Config: config.ResourceManagerConfigOptimus{
					Host: "sample-host",
				},
			}

			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.NoError(t, err)
		})
		t.Run("should return error if the resource manager is unknown", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "invalid-sample",
				Config: config.ResourceManagerConfigOptimus{
					Host: "sample-host",
				},
			}
			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.ErrorContains(t, err, "resource manager invalid-sample is not recognized")
		})
		t.Run("should return error if unable to construct optimus resource manager", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "optimus",
			}
			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.ErrorContains(t, err, "host is empty")
		})
	})
}

// ResourceManager is an autogenerated mock type for the ResourceManager type
type ResourceManager struct {
	mock.Mock
}

// GetOptimusUpstreams provides a mock function with given fields: ctx, unresolvedDependency
func (_m *ResourceManager) GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, unresolvedDependency)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Upstream) []*job.Upstream); ok {
		r0 = rf(ctx, unresolvedDependency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Upstream) error); ok {
		r1 = rf(ctx, unresolvedDependency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
