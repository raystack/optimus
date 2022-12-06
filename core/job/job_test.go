package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

func TestEntityJob(t *testing.T) {
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
	jobVersion, _ := job.VersionFrom(1)
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	jobTaskConfig, _ := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()

	specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
	jobADestination := job.ResourceURN("project.dataset.sample-a")
	jobASources := []job.ResourceURN{"project.dataset.sample-b"}
	jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

	specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
	jobBDestination := job.ResourceURN("project.dataset.sample-b")
	jobBSources := []job.ResourceURN{"project.dataset.sample-c"}
	jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBSources)

	t.Run("GetJobNames", func(t *testing.T) {
		t.Run("should return list of names", func(t *testing.T) {
			expectedJobNames := []job.Name{specA.Name(), specB.Name()}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			jobNames := jobs.GetJobNames()

			assert.EqualValues(t, expectedJobNames, jobNames)
		})
	})
	t.Run("GetNameAndSpecMap", func(t *testing.T) {
		t.Run("should return map with name as key and spec as value", func(t *testing.T) {
			expectedMap := map[job.Name]*job.Spec{
				specA.Name(): jobA.Spec(),
				specB.Name(): jobB.Spec(),
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetNameAndSpecMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})
	t.Run("GetNameAndJobMap", func(t *testing.T) {
		t.Run("should return map with name as key and job as value", func(t *testing.T) {
			expectedMap := map[job.Name]*job.Job{
				specA.Name(): jobA,
				specB.Name(): jobB,
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetNameAndJobMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})
	t.Run("GetNamespaceNameAndJobsMap", func(t *testing.T) {
		t.Run("should return map with namespace name as key and jobs as value", func(t *testing.T) {
			expectedMap := map[tenant.NamespaceName][]*job.Job{
				namespace.Name(): {jobA, jobB},
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetNamespaceNameAndJobsMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})
	t.Run("GetUnresolvedUpstreams", func(t *testing.T) {
		t.Run("should return upstreams with state unresolved", func(t *testing.T) {
			upstreamUnresolved1 := job.NewUpstreamUnresolved("job-B", "", project.Name())
			upstreamUnresolved2 := job.NewUpstreamUnresolved("", "project.dataset.sample-c", "")
			upstreamResolved := job.NewUpstreamResolved("job-d", "host-sample", "project.dataset.sample-d", sampleTenant, job.UpstreamTypeStatic, "", false)

			expected := []*job.Upstream{upstreamUnresolved1, upstreamUnresolved2}

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamUnresolved1, upstreamResolved, upstreamUnresolved2})

			unresolvedUpstreams := jobAWithUpstream.GetUnresolvedUpstreams()
			assert.EqualValues(t, expected, unresolvedUpstreams)
		})
	})
	t.Run("UpstreamTypeFrom", func(t *testing.T) {
		t.Run("should create static upstream type from string", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("static")
			assert.NoError(t, err)
			assert.Equal(t, job.UpstreamTypeStatic, upstreamType)
		})
		t.Run("should create inferred upstream type from string", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("inferred")
			assert.NoError(t, err)
			assert.Equal(t, job.UpstreamTypeInferred, upstreamType)
		})
		t.Run("should return error if the input is invalid", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("unrecognized type")
			assert.Empty(t, upstreamType)
			assert.ErrorContains(t, err, "unknown type for upstream")
		})
	})

	t.Run("ToUpstreamFullNameMap", func(t *testing.T) {
		t.Run("should return a map with full name as key and boolean as value", func(t *testing.T) {
			upstreamResolved1 := job.NewUpstreamResolved("job-a", "host-sample", "project.dataset.sample-a", sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", "project.dataset.sample-b", sampleTenant, job.UpstreamTypeInferred, "", false)

			expectedMap := map[string]bool{
				"test-proj/job-a": true,
				"test-proj/job-b": true,
			}

			upstreams := job.Upstreams([]*job.Upstream{upstreamResolved1, upstreamResolved2})
			resultMap := upstreams.ToUpstreamFullNameMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("ToUpstreamDestinationMap", func(t *testing.T) {
		t.Run("should return a map with destination resource urn as key and boolean as value", func(t *testing.T) {
			upstreamResolved1 := job.NewUpstreamResolved("job-a", "host-sample", "project.dataset.sample-a", sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", "project.dataset.sample-b", sampleTenant, job.UpstreamTypeInferred, "", false)

			expectedMap := map[job.ResourceURN]bool{
				"project.dataset.sample-a": true,
				"project.dataset.sample-b": true,
			}

			upstreams := job.Upstreams([]*job.Upstream{upstreamResolved1, upstreamResolved2})
			resultMap := upstreams.ToUpstreamDestinationMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("FullNameFrom", func(t *testing.T) {
		t.Run("should return the job full name given project and job name", func(t *testing.T) {
			fullName := job.FullNameFrom(project.Name(), specA.Name())
			assert.Equal(t, job.FullName("test-proj/job-A"), fullName)
			assert.Equal(t, "test-proj/job-A", fullName.String())
		})
	})

	t.Run("Job", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			specUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-E"}).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specUpstream).Build()
			jobCDestination := job.ResourceURN("project.dataset.sample-c")
			jobCSources := []job.ResourceURN{"project.dataset.sample-d"}
			jobC := job.NewJob(sampleTenant, specC, jobCDestination, jobCSources)

			assert.Equal(t, sampleTenant, jobC.Tenant())
			assert.Equal(t, specC, jobC.Spec())
			assert.Equal(t, jobCSources, jobC.Sources())
			assert.Equal(t, jobCDestination, jobC.Destination())
			assert.Equal(t, project.Name(), jobC.ProjectName())
			assert.Equal(t, specC.Name().String(), jobC.GetName())
			assert.Equal(t, "test-proj/job-C", jobC.FullName())
			assert.Equal(t, specUpstream.UpstreamNames(), jobC.StaticUpstreamNames())
		})
	})

	t.Run("WithUpstream", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			upstreamResolved := job.NewUpstreamResolved("job-d", "host-sample", "project.dataset.sample-d", sampleTenant, job.UpstreamTypeStatic, "bq2bq", false)
			assert.Equal(t, job.Name("job-d"), upstreamResolved.Name())
			assert.Equal(t, "host-sample", upstreamResolved.Host())
			assert.Equal(t, job.ResourceURN("project.dataset.sample-d"), upstreamResolved.Resource())
			assert.Equal(t, job.UpstreamTypeStatic, upstreamResolved.Type())
			assert.Equal(t, job.UpstreamStateResolved, upstreamResolved.State())
			assert.Equal(t, project.Name(), upstreamResolved.ProjectName())
			assert.Equal(t, namespace.Name(), upstreamResolved.NamespaceName())
			assert.Equal(t, false, upstreamResolved.External())
			assert.Equal(t, job.TaskName("bq2bq"), upstreamResolved.TaskName())
			assert.Equal(t, "test-proj/job-d", upstreamResolved.FullName())

			upstreamUnresolved := job.NewUpstreamUnresolved("", "project.dataset.sample-c", "")

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamResolved, upstreamUnresolved})
			assert.Equal(t, jobA, jobAWithUpstream.Job())
			assert.EqualValues(t, []*job.Upstream{upstreamResolved, upstreamUnresolved}, jobAWithUpstream.Upstreams())
			assert.EqualValues(t, specA.Name(), jobAWithUpstream.Name())
		})
	})
}
