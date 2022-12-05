package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/models"
)

const jobDatetimeLayout = "2006-01-02"

type Spec struct {
	ID          uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name        string    `gorm:"not null" json:"name"`
	Version     int
	Owner       string
	Description string
	Labels      datatypes.JSON

	StartDate time.Time
	EndDate   *time.Time
	Interval  string

	// Behavior
	DependsOnPast bool `json:"depends_on_past"`
	CatchUp       bool `json:"catch_up"`
	Retry         datatypes.JSON
	Alert         datatypes.JSON

	// Upstreams
	StaticUpstreams pq.StringArray `gorm:"type:varchar(220)[]" json:"static_upstreams"`

	// ExternalUpstreams
	HTTPUpstreams datatypes.JSON `json:"http_upstreams"`

	TaskName   string
	TaskConfig datatypes.JSON

	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string

	Assets   datatypes.JSON
	Hooks    datatypes.JSON
	Metadata datatypes.JSON

	Destination string
	Sources     pq.StringArray `gorm:"type:varchar(300)[]"`

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

type Retry struct {
	Count              int   `json:"count"`
	Delay              int32 `json:"delay"`
	ExponentialBackoff bool  `json:"exponential_backoff"`
}

type Alert struct {
	On       string
	Config   map[string]string
	Channels []string
}

type Asset struct {
	Name  string
	Value string
}

type Hook struct {
	Name   string
	Config datatypes.JSON
}

type Metadata struct {
	Resource  *MetadataResource
	Scheduler map[string]string
}

type MetadataResource struct {
	Request *MetadataResourceConfig
	Limit   *MetadataResourceConfig
}

type MetadataResourceConfig struct {
	CPU    string
	Memory string
}

type Config struct {
	Configs map[string]string
}

func toStorageSpec(jobEntity *job.Job) (*Spec, error) {
	var err error

	jobSpec := jobEntity.Spec()

	var labelsBytes []byte
	if jobSpec.Labels() != nil {
		labelsBytes, err = json.Marshal(jobSpec.Labels())
		if err != nil {
			return nil, err
		}
	}

	startDate, err := time.Parse(jobDatetimeLayout, jobSpec.Schedule().StartDate().String())
	if err != nil {
		return nil, err
	}

	var endDate time.Time
	if jobSpec.Schedule().EndDate() != "" {
		endDate, err = time.Parse(jobDatetimeLayout, jobSpec.Schedule().EndDate().String())
		if err != nil {
			return nil, err
		}
	}

	retryBytes, err := toStorageRetry(jobSpec.Schedule().Retry())
	if err != nil {
		return nil, err
	}

	alertsBytes, err := toStorageAlerts(jobSpec.Alerts())
	if err != nil {
		return nil, err
	}

	taskConfigBytes, err := toConfig(jobSpec.Task().Config())
	if err != nil {
		return nil, err
	}

	var assetsBytes []byte
	if jobSpec.Asset() != nil {
		a, err := toStorageAsset(jobSpec.Asset().Assets())
		if err != nil {
			return nil, err
		}
		assetsBytes = a
	}

	hooksBytes, err := toStorageHooks(jobSpec.Hooks())
	if err != nil {
		return nil, err
	}

	metadataBytes, err := toStorageMetadata(jobSpec.Metadata())
	if err != nil {
		return nil, err
	}

	var staticUpstreams []string
	var httpUpstreamsInBytes []byte
	if jobSpec.Upstream() != nil {
		for _, name := range jobSpec.Upstream().UpstreamNames() {
			staticUpstreams = append(staticUpstreams, name.String())
		}
		if jobSpec.Upstream().HTTPUpstreams() != nil {
			httpUpstreamsInBytes, err = json.Marshal(jobSpec.Upstream().HTTPUpstreams())
			if err != nil {
				return nil, err
			}
		}
	}

	sources := make([]string, len(jobEntity.Sources()))
	for i, source := range jobEntity.Sources() {
		sources[i] = source.String()
	}

	return &Spec{
		Name:        jobSpec.Name().String(),
		Version:     jobSpec.Version().Int(),
		Owner:       jobSpec.Owner().String(),
		Description: jobSpec.Description(),
		Labels:      labelsBytes,
		Assets:      assetsBytes,
		Metadata:    metadataBytes,

		StartDate: startDate,
		EndDate:   &endDate,
		Interval:  jobSpec.Schedule().Interval(),

		TaskName:   jobSpec.Task().Name().String(),
		TaskConfig: taskConfigBytes,

		Hooks: hooksBytes,

		WindowSize:       jobSpec.Window().GetSize(),
		WindowOffset:     jobSpec.Window().GetOffset(),
		WindowTruncateTo: jobSpec.Window().GetTruncateTo(),

		DependsOnPast: jobSpec.Schedule().DependsOnPast(),
		CatchUp:       jobSpec.Schedule().CatchUp(),
		Retry:         retryBytes,
		Alert:         alertsBytes,

		StaticUpstreams: staticUpstreams,
		HTTPUpstreams:   httpUpstreamsInBytes,

		Destination: jobEntity.Destination().String(),
		Sources:     sources,

		ProjectName:   jobEntity.Tenant().ProjectName().String(),
		NamespaceName: jobEntity.Tenant().NamespaceName().String(),
	}, nil
}

func toStorageHooks(hookSpecs []*job.Hook) ([]byte, error) {
	if hookSpecs == nil {
		return nil, nil
	}
	var hooks []Hook
	for _, hookSpec := range hookSpecs {
		hook, err := toStorageHook(hookSpec)
		if err != nil {
			return nil, err
		}
		hooks = append(hooks, hook)
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return nil, err
	}
	return hooksJSON, nil
}

func toStorageHook(spec *job.Hook) (Hook, error) {
	configJSON, err := json.Marshal(spec.Config().Configs())
	if err != nil {
		return Hook{}, err
	}
	return Hook{
		Name:   spec.Name().String(),
		Config: configJSON,
	}, nil
}

func toStorageAsset(assetSpecs map[string]string) ([]byte, error) {
	var assets []Asset
	for key, val := range assetSpecs {
		assets = append(assets, Asset{Name: key, Value: val})
	}
	assetsJSON, err := json.Marshal(assets)
	if err != nil {
		return nil, err
	}
	return assetsJSON, nil
}

func toStorageAlerts(alertSpecs []*job.Alert) ([]byte, error) {
	if alertSpecs == nil {
		return nil, nil
	}
	var alerts []Alert
	for _, alertSpec := range alertSpecs {
		alerts = append(alerts, Alert{
			On:       string(alertSpec.On()),
			Config:   alertSpec.Config().Configs(),
			Channels: alertSpec.Channels(),
		})
	}
	return json.Marshal(alerts)
}

func toStorageRetry(retrySpec *job.Retry) ([]byte, error) {
	if retrySpec == nil {
		return nil, nil
	}
	retry := Retry{
		Count:              retrySpec.Count(),
		Delay:              retrySpec.Delay(),
		ExponentialBackoff: retrySpec.ExponentialBackoff(),
	}
	return json.Marshal(retry)
}

func toStorageMetadata(metadataSpec *job.Metadata) ([]byte, error) {
	if metadataSpec == nil {
		return nil, nil
	}
	metadataResource := &MetadataResource{
		Request: nil,
		Limit:   nil,
	}

	metadata := Metadata{
		Resource:  metadataResource,
		Scheduler: metadataSpec.Scheduler(),
	}
	return json.Marshal(metadata)
}

func toConfig(configSpec *job.Config) ([]byte, error) {
	if configSpec == nil {
		return nil, nil
	}
	config := Config{
		Configs: configSpec.Configs(),
	}
	return json.Marshal(config)
}

func fromStorageSpec(jobSpec *Spec) (*job.Spec, error) {
	startDate, err := job.ScheduleDateFrom(jobSpec.StartDate.Format(job.DateLayout))
	if err != nil {
		return nil, err
	}

	scheduleBuilder := job.NewScheduleBuilder(startDate).WithCatchUp(jobSpec.CatchUp).WithDependsOnPast(jobSpec.DependsOnPast).WithInterval(jobSpec.Interval)

	if !jobSpec.EndDate.IsZero() {
		endDate, err := job.ScheduleDateFrom(jobSpec.EndDate.Format(job.DateLayout))
		if err != nil {
			return nil, err
		}
		scheduleBuilder = scheduleBuilder.WithEndDate(endDate)
	}

	if jobSpec.Retry != nil {
		var storageRetry Retry
		if err := json.Unmarshal(jobSpec.Retry, &storageRetry); err != nil {
			return nil, err
		}
		retry := job.NewRetry(storageRetry.Count, storageRetry.Delay, storageRetry.ExponentialBackoff)
		scheduleBuilder = scheduleBuilder.WithRetry(retry)
	}

	schedule, err := scheduleBuilder.Build()
	if err != nil {
		return nil, err
	}

	window, err := models.NewWindow(
		jobSpec.Version,
		jobSpec.WindowTruncateTo,
		jobSpec.WindowOffset,
		jobSpec.WindowSize,
	)
	if err != nil {
		return nil, err
	}

	var taskConfig *job.Config
	if jobSpec.TaskConfig != nil {
		storageTaskConfig := &Config{}
		if err := json.Unmarshal(jobSpec.TaskConfig, storageTaskConfig); err != nil {
			return nil, err
		}
		taskConfig, err = job.NewConfig(storageTaskConfig.Configs)
		if err != nil {
			return nil, err
		}
	}
	taskName, err := job.TaskNameFrom(jobSpec.TaskName)
	if err != nil {
		return nil, err
	}
	task := job.NewTaskBuilder(taskName, taskConfig).Build()

	var labels map[string]string
	if jobSpec.Labels != nil {
		if err := json.Unmarshal(jobSpec.Labels, &labels); err != nil {
			return nil, err
		}
	}

	hooks, err := fromStorageHooks(jobSpec.Hooks)
	if err != nil {
		return nil, err
	}

	alerts, err := fromStorageAlerts(jobSpec.Alert)
	if err != nil {
		return nil, err
	}

	var httpUpstreams []*job.SpecHTTPUpstream
	if jobSpec.HTTPUpstreams != nil {
		if err := json.Unmarshal(jobSpec.HTTPUpstreams, &httpUpstreams); err != nil {
			return nil, err
		}
	}

	var upstreamNames []job.SpecUpstreamName
	for _, staticUpstream := range jobSpec.StaticUpstreams {
		upstreamNames = append(upstreamNames, job.SpecUpstreamNameFrom(staticUpstream))
	}

	var upstreams *job.SpecUpstream
	if upstreamNames != nil || httpUpstreams != nil {
		upstreams = job.NewSpecUpstreamBuilder().WithUpstreamNames(upstreamNames).WithSpecHTTPUpstream(httpUpstreams).Build()
	}

	var metadata *job.Metadata
	if jobSpec.Metadata != nil {
		var storeMetadata Metadata
		if err := json.Unmarshal(jobSpec.Metadata, &storeMetadata); err != nil {
			return nil, err
		}
		metadataBuilder := job.NewMetadataBuilder()
		if storeMetadata.Resource != nil {
			var resourceRequest *job.MetadataResourceConfig
			if storeMetadata.Resource.Request != nil {
				resourceRequest = job.NewMetadataResourceConfig(storeMetadata.Resource.Request.CPU, storeMetadata.Resource.Request.Memory)
			}
			var resourceLimit *job.MetadataResourceConfig
			if storeMetadata.Resource.Limit != nil {
				resourceLimit = job.NewMetadataResourceConfig(storeMetadata.Resource.Limit.CPU, storeMetadata.Resource.Limit.Memory)
			}
			resourceMetadata := job.NewResourceMetadata(resourceRequest, resourceLimit)
			metadataBuilder = metadataBuilder.WithResource(resourceMetadata)
		}
		if storeMetadata.Scheduler != nil {
			metadataBuilder = metadataBuilder.WithScheduler(storeMetadata.Scheduler)
		}
		metadata = metadataBuilder.Build()
	}

	version, err := job.VersionFrom(jobSpec.Version)
	if err != nil {
		return nil, err
	}

	jobName, err := job.NameFrom(jobSpec.Name)
	if err != nil {
		return nil, err
	}

	owner, err := job.OwnerFrom(jobSpec.Owner)
	if err != nil {
		return nil, err
	}

	var asset *job.Asset
	if jobSpec.Assets != nil {
		assetsMap, err := fromStorageAssets(jobSpec.Assets)
		if err != nil {
			return nil, err
		}
		asset, err = job.NewAsset(assetsMap)
		if err != nil {
			return nil, err
		}
	}

	return job.NewSpecBuilder(
		version,
		jobName,
		owner,
		schedule,
		window,
		task,
	).
		WithDescription(jobSpec.Description).
		WithLabels(labels).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithSpecUpstream(upstreams).
		WithMetadata(metadata).
		WithAsset(asset).
		Build(), nil
}

func fromStorageHooks(raw []byte) ([]*job.Hook, error) {
	if raw == nil {
		return nil, nil
	}

	hooks := []Hook{}
	if err := json.Unmarshal(raw, &hooks); err != nil {
		return nil, err
	}

	jobHooks := []*job.Hook{}
	for _, hook := range hooks {
		jobHook, err := fromStorageHook(hook)
		if err != nil {
			return nil, err
		}
		jobHooks = append(jobHooks, jobHook)
	}

	return jobHooks, nil
}

func fromStorageHook(hook Hook) (*job.Hook, error) {
	mapConfig := map[string]string{}
	if err := json.Unmarshal(hook.Config, &mapConfig); err != nil {
		return nil, err
	}
	config, err := job.NewConfig(mapConfig)
	if err != nil {
		return nil, err
	}
	hookName, err := job.HookNameFrom(hook.Name)
	if err != nil {
		return nil, err
	}
	return job.NewHook(hookName, config), nil
}

func fromStorageAlerts(raw []byte) ([]*job.Alert, error) {
	if raw == nil {
		return nil, nil
	}

	alerts := []Alert{}
	if err := json.Unmarshal(raw, &alerts); err != nil {
		return nil, err
	}

	jobAlerts := []*job.Alert{}
	for _, alert := range alerts {
		config, err := job.NewConfig(alert.Config)
		if err != nil {
			return nil, err
		}
		jobAlert := job.NewAlertBuilder(job.EventType(alert.On), alert.Channels).
			WithConfig(config).
			Build()
		jobAlerts = append(jobAlerts, jobAlert)
	}

	return jobAlerts, nil
}

func fromStorageAssets(raw []byte) (map[string]string, error) {
	jobAssets := make(map[string]string)
	assets := []Asset{}
	if err := json.Unmarshal(raw, &assets); err != nil {
		return nil, err
	}

	if len(assets) == 0 {
		return jobAssets, nil
	}

	for _, asset := range assets {
		jobAssets[asset.Name] = asset.Value
	}
	return jobAssets, nil
}
