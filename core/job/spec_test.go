package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/models"
)

func TestEntitySpec(t *testing.T) {
	jobVersion := 1
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
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobTaskConfig, _ := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)
	description := "sample description"
	labels := map[string]string{"key": "value"}
	hook, _ := job.NewHook("sample-hook", jobTaskConfig)
	jobAlertConfig, _ := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})

	httpUpstreamConfig := map[string]string{"host": "sample-host"}
	httpUpstreamHeader := map[string]string{"header-key": "sample-header-val"}
	httpUpstream, _ := job.NewSpecHTTPUpstreamBuilder("sample-name", "sample-url").WithParams(httpUpstreamConfig).WithHeaders(httpUpstreamHeader).Build()
	specUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-d"}).WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
	alert, _ := job.NewAlertSpec("sla_miss", []string{"sample-channel"}, jobAlertConfig)
	assetMap := map[string]string{"key": "value"}
	asset, _ := job.AssetFrom(assetMap)
	resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	jobMetadata, _ := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"scheduler_config_key": "value"}).
		Build()

	t.Run("Spec", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.AlertSpec{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)

			assert.Equal(t, job.Name("job-A"), specA.Name())
			assert.Equal(t, jobVersion, specA.Version())
			assert.Equal(t, "sample-owner", specA.Owner())

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
			assert.Equal(t, jobTask.Config(), specA.Task().Config())
			assert.Equal(t, jobTask.Config(), specA.Task().Config())

			assert.Equal(t, description, specA.Description())
			assert.Equal(t, labels, specA.Labels())

			assert.Equal(t, []*job.Hook{hook}, specA.Hooks())
			assert.Equal(t, hook.Name(), specA.Hooks()[0].Name())
			assert.Equal(t, hook.Name(), specA.Hooks()[0].Name())
			assert.Equal(t, hook.Config(), specA.Hooks()[0].Config())
			assert.Equal(t, hook.Config(), specA.Hooks()[0].Config())

			assert.Equal(t, []*job.AlertSpec{alert}, specA.AlertSpecs())
			assert.Equal(t, alert.Config(), specA.AlertSpecs()[0].Config())
			assert.Equal(t, alert.Config(), specA.AlertSpecs()[0].Config())
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
			assert.Equal(t, asset, specA.Asset())

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
			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			specB, err := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			expectedMap := map[job.Name]*job.Spec{
				specA.Name(): specA,
				specB.Name(): specB,
			}

			specs := job.Specs([]*job.Spec{specA, specB})
			resultMap := specs.ToNameAndSpecMap()

			assert.EqualValues(t, expectedMap, resultMap)
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
			validAsset, err := job.AssetFrom(validAssetMap)
			assert.NoError(t, err)
			assert.Equal(t, job.Asset(validAssetMap), validAsset)
			assert.Equal(t, validAssetMap, validAsset.Map())
		})
		t.Run("should return nil and error if asset map is invalid", func(t *testing.T) {
			invalidAssetMap := map[string]string{"": ""}
			invalidAsset, err := job.AssetFrom(invalidAssetMap)
			assert.Error(t, err)
			assert.Nil(t, invalidAsset)
		})
	})

	t.Run("NameFrom", func(t *testing.T) {
		t.Run("should return error if name is empty", func(t *testing.T) {
			name, err := job.NameFrom("")
			assert.ErrorContains(t, err, "name is empty")
			assert.Empty(t, name)
		})
	})

	t.Run("ScheduleDateFrom", func(t *testing.T) {
		t.Run("should not return error if date is empty", func(t *testing.T) {
			scheduleDate, err := job.ScheduleDateFrom("")
			assert.NoError(t, err)
			assert.Empty(t, scheduleDate)
		})
		t.Run("should return error if date is invalid", func(t *testing.T) {
			scheduleDate, err := job.ScheduleDateFrom("invalid date format")
			assert.ErrorContains(t, err, "error is encountered when validating date with layout")
			assert.Empty(t, scheduleDate)
		})
	})

	t.Run("TaskNameFrom", func(t *testing.T) {
		t.Run("should return error if task name is empty", func(t *testing.T) {
			owner, err := job.TaskNameFrom("")
			assert.ErrorContains(t, err, "task name is empty")
			assert.Empty(t, owner)
		})
	})

	t.Run("ConfigFrom", func(t *testing.T) {
		t.Run("should return error if the config map is invalid", func(t *testing.T) {
			jobConfig, err := job.ConfigFrom(map[string]string{"": ""})
			assert.Error(t, err)
			assert.Empty(t, jobConfig)
		})
	})

	t.Run("NewLabels", func(t *testing.T) {
		t.Run("should return error if the labels map is invalid", func(t *testing.T) {
			jobLabels, err := job.NewLabels(map[string]string{"": ""})
			assert.Error(t, err)
			assert.Empty(t, jobLabels)
		})
	})
}
