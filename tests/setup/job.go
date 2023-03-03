package setup

import (
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/models"
)

type DummyJobBuilder struct {
	version     int
	owner       string
	description string

	retry     *job.Retry
	startDate job.ScheduleDate

	window models.Window

	taskConfig job.Config
	taskName   job.TaskName

	labels map[string]string

	hookConfig job.Config
	hookName   string

	alertConfig       job.Config
	alertName         string
	alertChannelNames []string

	asset map[string]string

	resourceRequestConfig *job.MetadataResourceConfig
	resourceLimitConfig   *job.MetadataResourceConfig
	scheduler             map[string]string

	name job.Name

	destinationURN job.ResourceURN
	sourceURNs     []job.ResourceURN

	specUpstreamNames []job.SpecUpstreamName
	specHTTPUpstreams []*job.SpecHTTPUpstream
}

func NewDummyJobBuilder() *DummyJobBuilder {
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	if err != nil {
		panic(err)
	}

	version := 1
	window, err := models.NewWindow(version, "d", "24h", "24h")
	if err != nil {
		panic(err)
	}

	taskConfig, err := job.ConfigFrom(map[string]string{"sample_task_key": "sample_task_value"})
	if err != nil {
		panic(err)
	}
	taskName, err := job.TaskNameFrom("bq2bq")
	if err != nil {
		panic(err)
	}

	hookConfig, err := job.ConfigFrom(map[string]string{"sample_hook_key": "sample_hook_value"})
	if err != nil {
		panic(err)
	}

	alertConfig, err := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})
	if err != nil {
		panic(err)
	}

	asset, err := job.AssetFrom(map[string]string{"assets/query.sql": "select 1;"})
	if err != nil {
		panic(err)
	}

	name, err := job.NameFrom("sample_job")
	if err != nil {
		panic(err)
	}

	httpUpstreamName := "other_optimus"
	httpUpstreamHost := "http://localhost:9000"
	httpUpstreamHeaders := map[string]string{
		"request_type": "api",
	}
	httpUpstreamParams := map[string]string{
		"name": "sample_job",
	}
	specHTTPUpstream, err := job.NewSpecHTTPUpstreamBuilder(httpUpstreamName, httpUpstreamHost).
		WithHeaders(httpUpstreamHeaders).
		WithParams(httpUpstreamParams).
		Build()
	if err != nil {
		panic(err)
	}

	return &DummyJobBuilder{
		version:     version,
		owner:       "dev_test",
		description: "description for job",
		retry:       job.NewRetry(5, 0, false),
		startDate:   startDate,
		window:      window,
		taskConfig:  taskConfig,
		taskName:    taskName,
		labels: map[string]string{
			"environment": "production",
		},
		hookConfig:            hookConfig,
		hookName:              "sample_hook",
		alertConfig:           alertConfig,
		alertName:             "sample_alert",
		alertChannelNames:     []string{"sample_channel"},
		asset:                 asset,
		resourceRequestConfig: job.NewMetadataResourceConfig("128m", "128Mi"),
		resourceLimitConfig:   job.NewMetadataResourceConfig("128m", "128Mi"),
		scheduler:             map[string]string{"scheduler_config_key": "value"},
		name:                  name,
		destinationURN:        job.ResourceURN("sample_job_destination"),
		sourceURNs:            []job.ResourceURN{"source_of_sample_job"},
		specUpstreamNames:     []job.SpecUpstreamName{job.SpecUpstreamNameFrom("smpale_job_upstream")},
		specHTTPUpstreams:     []*job.SpecHTTPUpstream{specHTTPUpstream},
	}
}

func (d *DummyJobBuilder) OverrideVersion(version int) *DummyJobBuilder {
	output := *d
	output.version = version
	return &output
}

func (d *DummyJobBuilder) OverrideOwner(owner string) *DummyJobBuilder {
	output := *d
	output.owner = owner
	return &output
}

func (d *DummyJobBuilder) OverrideDescription(description string) *DummyJobBuilder {
	output := *d
	output.description = description
	return &output
}

func (d *DummyJobBuilder) OverrideRetry(retry *job.Retry) *DummyJobBuilder {
	output := *d
	output.retry = retry
	return &output
}

func (d *DummyJobBuilder) OverrideStartDate(startDate job.ScheduleDate) *DummyJobBuilder {
	output := *d
	output.startDate = startDate
	return &output
}

func (d *DummyJobBuilder) OverrideWindow(window models.Window) *DummyJobBuilder {
	output := *d
	output.window = window
	return &output
}

func (d *DummyJobBuilder) OverrideTaskConfig(taskConfig job.Config) *DummyJobBuilder {
	output := *d
	output.taskConfig = taskConfig
	return &output
}

func (d *DummyJobBuilder) OverrideTaskName(taskName job.TaskName) *DummyJobBuilder {
	output := *d
	output.taskName = taskName
	return &output
}

func (d *DummyJobBuilder) OverrideLabels(labels map[string]string) *DummyJobBuilder {
	output := *d
	output.labels = labels
	return &output
}

func (d *DummyJobBuilder) OverrideHookConfig(hookConfig job.Config) *DummyJobBuilder {
	output := *d
	output.hookConfig = hookConfig
	return &output
}

func (d *DummyJobBuilder) OverrideHookName(hookName string) *DummyJobBuilder {
	output := *d
	output.hookName = hookName
	return &output
}

func (d *DummyJobBuilder) OverrideAlertConfig(alertConfig job.Config) *DummyJobBuilder {
	output := *d
	output.alertConfig = alertConfig
	return &output
}

func (d *DummyJobBuilder) OverrideAlertName(alertName string) *DummyJobBuilder {
	output := *d
	output.alertName = alertName
	return &output
}

func (d *DummyJobBuilder) OverrideAlertChannelNames(alertChannelNames []string) *DummyJobBuilder {
	output := *d
	output.alertChannelNames = alertChannelNames
	return &output
}

func (d *DummyJobBuilder) OverrideAsset(asset map[string]string) *DummyJobBuilder {
	output := *d
	output.asset = asset
	return &output
}

func (d *DummyJobBuilder) OverrideResourceRequestConfig(config *job.MetadataResourceConfig) *DummyJobBuilder {
	output := *d
	output.resourceRequestConfig = config
	return &output
}

func (d *DummyJobBuilder) OverrideResourceLimitConfig(config *job.MetadataResourceConfig) *DummyJobBuilder {
	output := *d
	output.resourceLimitConfig = config
	return &output
}

func (d *DummyJobBuilder) OverrideScheduler(scheduler map[string]string) *DummyJobBuilder {
	output := *d
	output.scheduler = scheduler
	return &output
}

func (d *DummyJobBuilder) OverrideName(name job.Name) *DummyJobBuilder {
	output := *d
	output.name = name
	return &output
}

func (d *DummyJobBuilder) OverrideDestinationURN(destinationURN job.ResourceURN) *DummyJobBuilder {
	output := *d
	output.destinationURN = destinationURN
	return &output
}

func (d *DummyJobBuilder) OverrideSourceURNs(sourceURNs []job.ResourceURN) *DummyJobBuilder {
	output := *d
	output.sourceURNs = sourceURNs
	return &output
}

func (d *DummyJobBuilder) OverrideSpecUpstreamNames(specUpstreamNames []job.SpecUpstreamName) *DummyJobBuilder {
	output := *d
	output.specUpstreamNames = specUpstreamNames
	return &output
}

func (d *DummyJobBuilder) OverrideSpecHTTPUpstreams(specHTTPUpstreams []*job.SpecHTTPUpstream) *DummyJobBuilder {
	output := *d
	output.specHTTPUpstreams = specHTTPUpstreams
	return &output
}

func (d *DummyJobBuilder) Build(tnnt tenant.Tenant) *job.Job {
	schedule, err := job.NewScheduleBuilder(d.startDate).WithRetry(d.retry).Build()
	if err != nil {
		panic(err)
	}
	task := job.NewTask(d.taskName, d.taskConfig)

	hook, err := job.NewHook(d.hookName, d.hookConfig)
	if err != nil {
		panic(err)
	}
	hooks := []*job.Hook{hook}

	alert, err := job.NewAlertSpec(d.alertName, d.alertChannelNames, d.alertConfig)
	if err != nil {
		panic(err)
	}
	alerts := []*job.AlertSpec{alert}

	resourceMetadata := job.NewResourceMetadata(d.resourceRequestConfig, d.resourceLimitConfig)
	metadata, err := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(d.scheduler).
		Build()
	if err != nil {
		panic(err)
	}

	specUpstream, err := job.NewSpecUpstreamBuilder().
		WithUpstreamNames(d.specUpstreamNames).
		WithSpecHTTPUpstream(d.specHTTPUpstreams).
		Build()
	if err != nil {
		panic(err)
	}

	spec, err := job.NewSpecBuilder(d.version, d.name, d.owner, schedule, d.window, task).
		WithDescription(d.description).
		WithLabels(d.labels).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithAsset(d.asset).
		WithMetadata(metadata).
		WithSpecUpstream(specUpstream).
		Build()
	if err != nil {
		panic(err)
	}
	return job.NewJob(tnnt, spec, d.destinationURN, d.sourceURNs)
}
