package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

func TestEntitySpec(t *testing.T) {
	jobVersion, _ := job.VersionFrom(1)
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	endDate, _ := job.ScheduleDateFrom("2022-10-02")
	retry := job.NewRetry(0, int32(0), false)
	jobSchedule, _ := job.NewScheduleBuilder(startDate).
		WithEndDate(endDate).
		WithInterval("0 2 * * *").
		WithCatchUp(true).
		WithRetry(retry).
		WithDependsOnPast(false).
		Build()
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	jobTaskConfig, _ := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()
	description := "sample description"
	labels := map[string]string{"key": "value"}
	hook := job.NewHook("sample-hook", jobTaskConfig)
	jobAlertConfig, _ := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})

	httpUpstreamConfig := map[string]string{"host": "sample-host"}
	httpUpstreamHeader := map[string]string{"header-key": "sample-header-val"}
	httpUpstream, _ := job.NewSpecHTTPUpstreamBuilder("sample-name", "sample-url").WithParams(httpUpstreamConfig).WithHeaders(httpUpstreamHeader).Build()
	specUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-d"}).WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
	alert, _ := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
	assetMap := map[string]string{"key": "value"}
	asset, _ := job.NewAsset(assetMap)
	resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	jobMetadata, _ := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"scheduler_config_key": "value"}).
		Build()

	t.Run("Spec", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			specA := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.AlertSpec{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			assert.Equal(t, job.Name("job-A"), specA.Name())
			assert.Equal(t, jobVersion, specA.Version())
			assert.Equal(t, job.Owner("sample-owner"), specA.Owner())
			assert.Equal(t, "sample-owner", specA.Owner().String())

			assert.Equal(t, jobSchedule, specA.Schedule())
			assert.Equal(t, jobSchedule.Retry(), specA.Schedule().Retry())
			assert.Equal(t, jobSchedule.Retry().Delay(), specA.Schedule().Retry().Delay())
			assert.Equal(t, jobSchedule.Retry().Count(), specA.Schedule().Retry().Count())
			assert.Equal(t, jobSchedule.Retry().ExponentialBackoff(), specA.Schedule().Retry().ExponentialBackoff())
			assert.Equal(t, jobSchedule.EndDate(), specA.Schedule().EndDate())
			assert.Equal(t, jobSchedule.StartDate(), specA.Schedule().StartDate())
			assert.Equal(t, jobSchedule.StartDate().String(), specA.Schedule().StartDate().String())
			assert.Equal(t, jobSchedule.DependsOnPast(), specA.Schedule().DependsOnPast())
			assert.Equal(t, jobSchedule.CatchUp(), specA.Schedule().CatchUp())
			assert.Equal(t, jobSchedule.Interval(), specA.Schedule().Interval())

			assert.Equal(t, jobWindow, specA.Window())

			assert.Equal(t, jobTask, specA.Task())
			assert.Equal(t, jobTask.Name(), specA.Task().Name())
			assert.Equal(t, jobTask.Name().String(), specA.Task().Name().String())
			assert.Equal(t, jobTask.Info(), specA.Task().Info())
			assert.Equal(t, jobTask.Config(), specA.Task().Config())
			assert.Equal(t, jobTask.Config().Configs(), specA.Task().Config().Configs())

			assert.Equal(t, description, specA.Description())
			assert.Equal(t, labels, specA.Labels())

			assert.Equal(t, []*job.Hook{hook}, specA.Hooks())
			assert.Equal(t, hook.Name(), specA.Hooks()[0].Name())
			assert.Equal(t, hook.Name().String(), specA.Hooks()[0].Name().String())
			assert.Equal(t, hook.Config(), specA.Hooks()[0].Config())
			assert.Equal(t, hook.Config().Configs(), specA.Hooks()[0].Config().Configs())

			assert.Equal(t, []*job.AlertSpec{alert}, specA.AlertSpecs())
			assert.Equal(t, alert.Config(), specA.AlertSpecs()[0].Config())
			assert.Equal(t, alert.Config().Configs(), specA.AlertSpecs()[0].Config().Configs())
			assert.Equal(t, alert.Channels(), specA.AlertSpecs()[0].Channels())
			assert.Equal(t, alert.On(), specA.AlertSpecs()[0].On())

			assert.Equal(t, specUpstream, specA.UpstreamSpec())
			assert.Equal(t, specUpstream.UpstreamNames(), specA.UpstreamSpec().UpstreamNames())
			assert.Equal(t, specUpstream.HTTPUpstreams(), specA.UpstreamSpec().HTTPUpstreams())
			assert.Equal(t, specUpstream.HTTPUpstreams()[0].URL(), specA.UpstreamSpec().HTTPUpstreams()[0].URL())
			assert.Equal(t, specUpstream.HTTPUpstreams()[0].Name(), specA.UpstreamSpec().HTTPUpstreams()[0].Name())
			assert.Equal(t, specUpstream.HTTPUpstreams()[0].Params(), specA.UpstreamSpec().HTTPUpstreams()[0].Params())
			assert.Equal(t, specUpstream.HTTPUpstreams()[0].Headers(), specA.UpstreamSpec().HTTPUpstreams()[0].Headers())

			assert.Equal(t, asset, specA.Asset())
			assert.Equal(t, asset.Assets(), specA.Asset().Assets())

			assert.Equal(t, jobMetadata, specA.Metadata())
			assert.Equal(t, jobMetadata.Resource(), specA.Metadata().Resource())
			assert.Equal(t, jobMetadata.Resource().Request().CPU(), specA.Metadata().Resource().Request().CPU())
			assert.Equal(t, jobMetadata.Resource().Request().Memory(), specA.Metadata().Resource().Request().Memory())
			assert.Equal(t, jobMetadata.Resource().Limit().CPU(), specA.Metadata().Resource().Limit().CPU())
			assert.Equal(t, jobMetadata.Resource().Limit().Memory(), specA.Metadata().Resource().Limit().Memory())
			assert.Equal(t, jobMetadata.Scheduler(), specA.Metadata().Scheduler())
			assert.Equal(t, jobMetadata.Scheduler(), specA.Metadata().Scheduler())
		})
	})

	t.Run("Specs", func(t *testing.T) {
		t.Run("ToNameAndSpecMap should return map with name key and spec value", func(t *testing.T) {
			specA := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			expectedMap := map[job.Name]*job.Spec{
				specA.Name(): specA,
				specB.Name(): specB,
			}

			specs := job.Specs([]*job.Spec{specA, specB})
			resultMap := specs.ToNameAndSpecMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("IsEqual", func(t *testing.T) {
		t.Run("should return true if specs are equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.AlertSpec{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.AlertSpec{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.True(t, equal)
		})
		t.Run("should return false if version is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			jobVersion2, err := job.VersionFrom(2)
			assert.NoError(t, err)
			incomingSpec := job.NewSpecBuilder(jobVersion2, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if name is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			jobName, err := job.NameFrom("job-B")
			assert.NoError(t, err)
			incomingSpec := job.NewSpecBuilder(jobVersion, jobName, "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if owner is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			owner, err := job.OwnerFrom("sample-owner-2")
			assert.NoError(t, err)
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", owner, jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if schedule is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			updatedEndDate, err := job.ScheduleDateFrom("2022-01-01")
			assert.NoError(t, err)
			jobScheduleUpdated, _ := job.NewScheduleBuilder(startDate).WithEndDate(updatedEndDate).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobScheduleUpdated, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if window is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			jobWindowUpdated, _ := models.NewWindow(jobVersion.Int(), "h", "24h", "24h")
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindowUpdated, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if task is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			taskNameUpdated, err := job.TaskNameFrom("changed-task")
			assert.NoError(t, err)
			jobTaskUpdated := job.NewTaskBuilder(taskNameUpdated, jobTaskConfig).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if description is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithDescription(description).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithDescription("updated description").Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if labels are not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithLabels(labels).Build()

			labelsUpdated := map[string]string{"key": "value2"}
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithLabels(labelsUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if metadata is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithMetadata(jobMetadata).Build()

			jobMetadataUpdated, _ := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value2"}).
				Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithMetadata(jobMetadataUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if hooks are not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithHooks([]*job.Hook{hook}).Build()

			hookNameUpdated, err := job.HookNameFrom("sample-hook2")
			assert.NoError(t, err)
			hookUpdated := job.NewHook(hookNameUpdated, jobTaskConfig)
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithHooks([]*job.Hook{hookUpdated}).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if assets are not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(asset).Build()

			assetUpdated, _ := job.NewAsset(map[string]string{"key2": "value2"})
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(assetUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if upstreams are not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specUpstream).Build()

			specUpstreamUpdated, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-e"}).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specUpstreamUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
	})

	t.Run("SpecUpstreamName", func(t *testing.T) {
		t.Run("IsWithProjectName", func(t *testing.T) {
			t.Run("returns true if includes project name", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-project/sample-job")
				assert.True(t, upstreamName.IsWithProjectName())
			})
			t.Run("returns false if not include project name", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-job")
				assert.False(t, upstreamName.IsWithProjectName())
			})
		})
		t.Run("GetProjectName", func(t *testing.T) {
			t.Run("returns project name if exist", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-project/sample-job")
				projectName, err := upstreamName.GetProjectName()
				assert.NoError(t, err)
				assert.Equal(t, tenant.ProjectName("sample-project"), projectName)
			})
			t.Run("returns false if not include project name", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-job")
				projectName, err := upstreamName.GetProjectName()
				assert.Empty(t, projectName)
				assert.ErrorContains(t, err, "project name in job upstream specification not found")
			})
		})
		t.Run("GetJobName", func(t *testing.T) {
			t.Run("returns job name for upstream with project name specified", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-project/sample-job")
				jobName, err := upstreamName.GetJobName()
				assert.NoError(t, err)
				assert.Equal(t, job.Name("sample-job"), jobName)
			})
			t.Run("returns job name for upstream with project name not specified", func(t *testing.T) {
				upstreamName := job.SpecUpstreamNameFrom("sample-job")
				jobName, err := upstreamName.GetJobName()
				assert.NoError(t, err)
				assert.Equal(t, job.Name("sample-job"), jobName)
			})
		})
	})

	t.Run("Metadata", func(t *testing.T) {
		t.Run("should return nil if no error found", func(t *testing.T) {
			schedulerConf := map[string]string{"key": "val"}
			validJobMetadata, err := job.NewMetadataBuilder().
				WithScheduler(schedulerConf).
				Build()
			assert.NoError(t, err)
			assert.Equal(t, schedulerConf, validJobMetadata.Scheduler())
		})
		t.Run("should return error if metadata is invalid", func(t *testing.T) {
			invalidJobMetadata, err := job.NewMetadataBuilder().
				WithScheduler(map[string]string{"": ""}).
				Build()
			assert.Error(t, err)
			assert.Nil(t, invalidJobMetadata)
		})
	})

	t.Run("Asset", func(t *testing.T) {
		t.Run("should return asset and nil error if no error found", func(t *testing.T) {
			validAssetMap := map[string]string{"key": "value"}
			validAsset, err := job.NewAsset(validAssetMap)
			assert.NoError(t, err)
			assert.Equal(t, validAssetMap, validAsset.Assets())
		})
		t.Run("should return nil and error if asset map is invalid", func(t *testing.T) {
			invalidAssetMap := map[string]string{"": ""}
			invalidAsset, err := job.NewAsset(invalidAssetMap)
			assert.Error(t, err)
			assert.Nil(t, invalidAsset)
		})
	})
}
