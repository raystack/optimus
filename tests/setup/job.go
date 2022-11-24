package setup

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
)

func Job(tnnt tenant.Tenant, name job.Name) *job.Job {
	version, err := job.VersionFrom(1)
	if err != nil {
		panic(err)
	}
	owner, err := job.OwnerFrom("dev_test")
	if err != nil {
		panic(err)
	}
	description := "sample job"
	retry := job.NewRetry(5, 0, false)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	if err != nil {
		panic(err)
	}
	schedule, err := job.NewScheduleBuilder(startDate).WithRetry(retry).Build()
	if err != nil {
		panic(err)
	}
	window, err := models.NewWindow(version.Int(), "d", "24h", "24h")
	if err != nil {
		panic(err)
	}
	taskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	if err != nil {
		panic(err)
	}
	task := job.NewTaskBuilder("bq2bq", taskConfig).Build()

	labels := map[string]string{
		"environment": "integration",
	}
	hookConfig, err := job.NewConfig(map[string]string{"sample_hook_key": "sample_value"})
	if err != nil {
		panic(err)
	}
	hooks := []*job.Hook{job.NewHook("sample_hook", hookConfig)}
	alertConfig, err := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
	if err != nil {
		panic(err)
	}
	alert, err := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(alertConfig).Build()
	if err != nil {
		panic(err)
	}
	alerts := []*job.AlertSpec{alert}
	upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
	upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
	upstream, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
	if err != nil {
		panic(err)
	}
	asset, err := job.NewAsset(map[string]string{"sample-asset": "value-asset"})
	if err != nil {
		panic(err)
	}
	resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	metadata, err := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"scheduler_config_key": "value"}).
		Build()
	if err != nil {
		panic(err)
	}

	spec := job.NewSpecBuilder(version, name, owner, schedule, window, task).
		WithDescription(description).
		WithLabels(labels).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithSpecUpstream(upstream).
		WithAsset(asset).
		WithMetadata(metadata).
		Build()
	return job.NewJob(tnnt, spec, "dev.resource.sample", []job.ResourceURN{"resource"})
}
