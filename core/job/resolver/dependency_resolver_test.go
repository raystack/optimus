package resolver_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/job/resolver"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

func TestDependencyResolver(t *testing.T) {
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
	externalTenant, _ := tenant.NewTenant("external-proj", "external-namespace")
	jobVersion := 1
	jobSchedule := job.NewSchedule("2022-10-01", "", "", false, false, nil)
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobTaskConfig := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolve dependency internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			dependencySpec := job.NewDependencySpec([]string{"test-proj/job-c"}, nil)
			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, dependencySpec, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			dependencyB, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "inferred")
			dependencyC, _ := job.NewDependencyResolved("job-C", "", "resource-C", sampleTenant, "static")
			dependencies := []*job.Dependency{dependencyB, dependencyC}
			jobNameWithDependencies := map[job.Name][]*job.Dependency{
				jobA.JobSpec().Name(): dependencies,
			}

			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), []job.Name{jobSpecA.Name()}).Return(jobNameWithDependencies, nil)

			externalDependencyResolver.On("FetchExternalDependencies", ctx, mock.Anything).Return([]*job.Dependency{}, nil, nil)

			expectedJobWithDependencies := []*job.WithDependency{job.NewWithDependency(jobA, []*job.Dependency{dependencyB, dependencyC})}

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.Nil(t, depErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, expectedJobWithDependencies, result)
		})
		t.Run("resolve dependency internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			dependencySpec := job.NewDependencySpec([]string{"job-c"}, nil)
			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, dependencySpec, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B", "resource-D"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			internalDependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithDependencies := map[job.Name][]*job.Dependency{
				jobA.JobSpec().Name(): {internalDependency},
			}

			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), []job.Name{jobSpecA.Name()}).Return(jobNameWithDependencies, nil)

			externalDependencyC, _ := job.NewDependencyResolved("job-C", "external-host", "resource-C", externalTenant, "static")
			externalDependencyD, _ := job.NewDependencyResolved("job-D", "external-host", "resource-D", externalTenant, "inferred")
			externalDependencyResolver.On("FetchExternalDependencies", ctx, mock.Anything).Return([]*job.Dependency{externalDependencyC, externalDependencyD}, nil, nil)

			expectedJobWithDependencies := []*job.WithDependency{
				job.NewWithDependency(jobA, []*job.Dependency{internalDependency, externalDependencyC, externalDependencyD}),
			}

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.Nil(t, depErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, expectedJobWithDependencies, result)
		})
		t.Run("returns error when unable to get internal dependencies", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), mock.Anything).Return(map[job.Name][]*job.Dependency{}, errors.New("internal error"))

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.Nil(t, depErrors)
			assert.NotNil(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns dependency error when there is unresolved static dependency", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			dependencySpec := job.NewDependencySpec([]string{"job-c"}, nil)
			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, dependencySpec, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B", "resource-D"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			internalDependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithDependencies := map[job.Name][]*job.Dependency{
				jobA.JobSpec().Name(): {internalDependency},
			}

			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), []job.Name{jobSpecA.Name()}).Return(jobNameWithDependencies, nil)

			unresolvedDependencyC := &dto.RawDependency{
				ProjectName: externalTenant.ProjectName().String(),
				JobName:     "job-C",
			}
			unresolvedDependencyD := &dto.RawDependency{
				ResourceURN: "resource-D",
			}
			externalDependencyResolver.On("FetchExternalDependencies", ctx, mock.Anything).Return([]*job.Dependency{}, []*dto.RawDependency{unresolvedDependencyC, unresolvedDependencyD}, nil)

			externalDependencyC := job.NewDependencyUnresolved("job-C", "", "external-proj")
			externalDependencyD := job.NewDependencyUnresolved("", "resource-D", "")
			expectedJobWithDependencies := []*job.WithDependency{
				job.NewWithDependency(jobA, []*job.Dependency{internalDependency, externalDependencyC, externalDependencyD}),
			}

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.NotNil(t, depErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, expectedJobWithDependencies, result)
		})
		t.Run("returns dependency error when encounter error on fetching fetch external dependencies", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			dependencySpec := job.NewDependencySpec([]string{"job-c"}, nil)
			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, dependencySpec, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B", "resource-D"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			internalDependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithDependencies := map[job.Name][]*job.Dependency{
				jobA.JobSpec().Name(): {internalDependency},
			}

			externalDependencyC, _ := job.NewDependencyResolved("job-C", "external-host", "resource-C", externalTenant, "static")
			externalDependencyD, _ := job.NewDependencyResolved("job-D", "external-host", "resource-D", externalTenant, "inferred")
			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), []job.Name{jobSpecA.Name()}).Return(jobNameWithDependencies, nil)

			externalDependencyResolver.On("FetchExternalDependencies", ctx, mock.Anything).Return([]*job.Dependency{externalDependencyC, externalDependencyD}, nil, errors.New("internal error"))

			expectedJobWithDependencies := []*job.WithDependency{
				job.NewWithDependency(jobA, []*job.Dependency{internalDependency, externalDependencyC, externalDependencyD}),
			}

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.NotNil(t, depErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, expectedJobWithDependencies, result)
		})
		t.Run("returns dependency error when encounter error on initializing unresolved dependency", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalDependencyResolver := new(ExternalDependencyResolver)

			dependencySpec := job.NewDependencySpec([]string{"job-c"}, nil)
			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, dependencySpec, nil, nil)
			assert.Nil(t, err)
			jobADestination := "resource-A"
			jobADependencies := []string{"resource-B", "resource-D"}

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}

			internalDependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithDependencies := map[job.Name][]*job.Dependency{
				jobA.JobSpec().Name(): {internalDependency},
			}

			jobRepo.On("GetJobNameWithInternalDependencies", ctx, project.Name(), []job.Name{jobSpecA.Name()}).Return(jobNameWithDependencies, nil)

			unresolvedDependencyC := &dto.RawDependency{
				ProjectName: externalTenant.ProjectName().String(),
				JobName:     "job-C",
			}
			unresolvedDependencyD := &dto.RawDependency{
				ResourceURN: "resource-D",
			}
			externalDependencyResolver.On("FetchExternalDependencies", ctx, mock.Anything).Return([]*job.Dependency{}, []*dto.RawDependency{unresolvedDependencyC, unresolvedDependencyD}, nil)

			externalDependencyC := job.NewDependencyUnresolved("job-C", "", "external-proj")
			externalDependencyD := job.NewDependencyUnresolved("", "resource-D", "")
			expectedJobWithDependencies := []*job.WithDependency{
				job.NewWithDependency(jobA, []*job.Dependency{internalDependency, externalDependencyC, externalDependencyD}),
			}

			dependencyResolver := resolver.NewDependencyResolver(jobRepo, externalDependencyResolver)
			result, depErrors, err := dependencyResolver.Resolve(ctx, project.Name(), jobs)
			assert.NotNil(t, depErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, expectedJobWithDependencies, result)
		})
	})
}

// ExternalDependencyResolver is an autogenerated mock type for the ExternalDependencyResolver type
type ExternalDependencyResolver struct {
	mock.Mock
}

// FetchExternalDependencies provides a mock function with given fields: ctx, unresolvedDependencies
func (_m *ExternalDependencyResolver) FetchExternalDependencies(ctx context.Context, unresolvedDependencies []*dto.RawDependency) ([]*job.Dependency, []*dto.RawDependency, error) {
	ret := _m.Called(ctx, unresolvedDependencies)

	var r0 []*job.Dependency
	if rf, ok := ret.Get(0).(func(context.Context, []*dto.RawDependency) []*job.Dependency); ok {
		r0 = rf(ctx, unresolvedDependencies)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Dependency)
		}
	}

	var r1 []*dto.RawDependency
	if rf, ok := ret.Get(1).(func(context.Context, []*dto.RawDependency) []*dto.RawDependency); ok {
		r1 = rf(ctx, unresolvedDependencies)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*dto.RawDependency)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, []*dto.RawDependency) error); ok {
		r2 = rf(ctx, unresolvedDependencies)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// Add provides a mock function with given fields: ctx, jobs
func (_m *JobRepository) Add(ctx context.Context, jobs []*job.Job) ([]*job.Job, error, error) {
	ret := _m.Called(ctx, jobs)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(ctx, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(ctx, jobs)
	} else {
		r1 = ret.Error(1)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, []*job.Job) error); ok {
		r2 = rf(ctx, jobs)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// GetJobNameWithDependencies provides a mock function with given fields: ctx, projectName, jobNames
func (_m *JobRepository) GetJobNameWithInternalDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Dependency, error) {
	ret := _m.Called(ctx, projectName, jobNames)

	var r0 map[job.Name][]*job.Dependency
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Dependency); ok {
		r0 = rf(ctx, projectName, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Dependency)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []job.Name) error); ok {
		r1 = rf(ctx, projectName, jobNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
