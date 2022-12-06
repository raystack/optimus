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

	t.Run("Spec", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			description := "sample description"
			labels := map[string]string{"key": "value"}
			hook := job.NewHook("sample-hook", jobTaskConfig)

			jobAlertConfig, err := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)

			specUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-d"}).Build()

			alert := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()

			assetMap := map[string]string{"key": "value"}
			asset, err := job.NewAsset(assetMap)
			assert.NoError(t, err)

			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()

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
}
