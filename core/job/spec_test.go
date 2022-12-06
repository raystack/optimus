package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/models"
)

func TestEntitySpec(t *testing.T) {
	jobVersion, _ := job.VersionFrom(1)
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	jobTaskConfig, _ := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()
	description := "sample description"
	labels := map[string]string{"key": "value"}
	hook := job.NewHook("sample-hook", jobTaskConfig)
	jobAlertConfig, _ := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
	specUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-d"}).Build()
	alert := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
	assetMap := map[string]string{"key": "value"}
	asset, _ := job.NewAsset(assetMap)
	resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	jobMetadata := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"scheduler_config_key": "value"}).
		Build()

	t.Run("Spec", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			specA := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.Alert{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			assert.Equal(t, job.Name("job-A"), specA.Name())
			assert.Equal(t, jobVersion, specA.Version())
			assert.Equal(t, job.Owner("sample-owner"), specA.Owner())
			assert.Equal(t, jobSchedule, specA.Schedule())
			assert.Equal(t, jobWindow, specA.Window())
			assert.Equal(t, jobTask, specA.Task())
			assert.Equal(t, description, specA.Description())
			assert.Equal(t, labels, specA.Labels())
			assert.Equal(t, []*job.Hook{hook}, specA.Hooks())
			assert.Equal(t, []*job.Alert{alert}, specA.Alerts())
			assert.Equal(t, specUpstream, specA.Upstream())
			assert.Equal(t, asset, specA.Asset())
			assert.Equal(t, jobMetadata, specA.Metadata())
		})
	})

	t.Run("IsEqual", func(t *testing.T) {
		t.Run("should return true if specs are equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.Alert{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).
				WithDescription(description).
				WithLabels(labels).WithHooks([]*job.Hook{hook}).WithAlerts([]*job.Alert{alert}).
				WithSpecUpstream(specUpstream).
				WithAsset(asset).
				WithMetadata(jobMetadata).
				Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.True(t, equal)
		})
		t.Run("should return false if version is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			jobVersion2, _ := job.VersionFrom(2)
			incomingSpec := job.NewSpecBuilder(jobVersion2, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if name is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if owner is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-2", jobSchedule, jobWindow, jobTask).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if schedule is not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()

			jobScheduleUpdated, _ := job.NewScheduleBuilder(startDate).WithEndDate("2022-01-01").Build()
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

			jobTaskUpdated := job.NewTaskBuilder("changed-task", jobTaskConfig).Build()
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

			jobMetadataUpdated := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value2"}).
				Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithMetadata(jobMetadataUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
		t.Run("should return false if hooks are not equal", func(t *testing.T) {
			existingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithHooks([]*job.Hook{hook}).Build()

			hookUpdated := job.NewHook("sample-hook2", jobTaskConfig)
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

			specUpstreamUpdated := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-e"}).Build()
			incomingSpec := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specUpstreamUpdated).Build()

			equal := existingSpec.IsEqual(incomingSpec)
			assert.False(t, equal)
		})
	})
}
