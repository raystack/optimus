package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
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

	alertsBytes, err := toStorageAlerts(jobSpec.AlertSpecs())
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
	if jobSpec.UpstreamSpec() != nil {
		for _, name := range jobSpec.UpstreamSpec().UpstreamNames() {
			staticUpstreams = append(staticUpstreams, name.String())
		}
		if jobSpec.UpstreamSpec().HTTPUpstreams() != nil {
			httpUpstreamsInBytes, err = json.Marshal(jobSpec.UpstreamSpec().HTTPUpstreams())
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
	assetsJSON, err := json.Marshal(assetSpecs)
	if err != nil {
		return nil, err
	}
	return assetsJSON, nil
}

func toStorageAlerts(alertSpecs []*job.AlertSpec) ([]byte, error) {
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

	var metadataResource *MetadataResource
	if metadataSpec.Resource() != nil {
		var resourceRequest *MetadataResourceConfig
		if metadataSpec.Resource().Request() != nil {
			resourceRequest = &MetadataResourceConfig{
				CPU:    metadataSpec.Resource().Request().CPU(),
				Memory: metadataSpec.Resource().Request().Memory(),
			}
		}
		var resourceLimit *MetadataResourceConfig
		if metadataSpec.Resource().Limit() != nil {
			resourceLimit = &MetadataResourceConfig{
				CPU:    metadataSpec.Resource().Limit().CPU(),
				Memory: metadataSpec.Resource().Limit().Memory(),
			}
		}
		metadataResource = &MetadataResource{
			Request: resourceRequest,
			Limit:   resourceLimit,
		}
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
	return json.Marshal(configSpec.Configs())
}

func fromStorageSpec(jobSpec *Spec) (*job.Spec, error) {
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

	startDate, err := job.ScheduleDateFrom(jobSpec.StartDate.Format(job.DateLayout))
	if err != nil {
		return nil, err
	}

	scheduleBuilder := job.NewScheduleBuilder(startDate).
		WithCatchUp(jobSpec.CatchUp).
		WithDependsOnPast(jobSpec.DependsOnPast).
		WithInterval(jobSpec.Interval)

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
		var configMap map[string]string
		if err := json.Unmarshal(jobSpec.TaskConfig, &configMap); err != nil {
			return nil, err
		}
		taskConfig, err = job.NewConfig(configMap)
		if err != nil {
			return nil, err
		}
	}
	taskName, err := job.TaskNameFrom(jobSpec.TaskName)
	if err != nil {
		return nil, err
	}
	task := job.NewTaskBuilder(taskName, taskConfig).Build()
	jobSpecBuilder := job.NewSpecBuilder(version, jobName, owner, schedule, window, task).WithDescription(jobSpec.Description)

	if jobSpec.Labels != nil {
		var labels map[string]string
		if err := json.Unmarshal(jobSpec.Labels, &labels); err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithLabels(labels)
	}

	if jobSpec.Hooks != nil {
		hooks, err := fromStorageHooks(jobSpec.Hooks)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithHooks(hooks)
	}

	if jobSpec.Alert != nil {
		alerts, err := fromStorageAlerts(jobSpec.Alert)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithAlerts(alerts)
	}

	upstreamSpecBuilder := job.NewSpecUpstreamBuilder()
	var httpUpstreams []*job.SpecHTTPUpstream
	if jobSpec.HTTPUpstreams != nil {
		if err := json.Unmarshal(jobSpec.HTTPUpstreams, &httpUpstreams); err != nil {
			return nil, err
		}
		upstreamSpecBuilder = upstreamSpecBuilder.WithSpecHTTPUpstream(httpUpstreams)
	}

	var upstreamNames []job.SpecUpstreamName
	if jobSpec.StaticUpstreams != nil {
		for _, staticUpstream := range jobSpec.StaticUpstreams {
			upstreamNames = append(upstreamNames, job.SpecUpstreamNameFrom(staticUpstream))
		}
		upstreamSpecBuilder = upstreamSpecBuilder.WithUpstreamNames(upstreamNames)
	}

	if httpUpstreams != nil || upstreamNames != nil {
		upstreamSpec, err := upstreamSpecBuilder.Build()
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithSpecUpstream(upstreamSpec)
	}

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
		metadata, err := metadataBuilder.Build()
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithMetadata(metadata)
	}

	if jobSpec.Assets != nil {
		assetsMap, err := fromStorageAssets(jobSpec.Assets)
		if err != nil {
			return nil, err
		}
		asset, err := job.NewAsset(assetsMap)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithAsset(asset)
	}

	return jobSpecBuilder.Build(), nil
}

func fromStorageHooks(raw []byte) ([]*job.Hook, error) {
	if raw == nil {
		return nil, nil
	}

	var hooks []Hook
	if err := json.Unmarshal(raw, &hooks); err != nil {
		return nil, err
	}

	var jobHooks []*job.Hook
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
	var configMap map[string]string
	if err := json.Unmarshal(hook.Config, &configMap); err != nil {
		return nil, err
	}
	config, err := job.NewConfig(configMap)
	if err != nil {
		return nil, err
	}
	hookName, err := job.HookNameFrom(hook.Name)
	if err != nil {
		return nil, err
	}
	return job.NewHook(hookName, config), nil
}

func fromStorageAlerts(raw []byte) ([]*job.AlertSpec, error) {
	if raw == nil {
		return nil, nil
	}

	var alerts []Alert
	if err := json.Unmarshal(raw, &alerts); err != nil {
		return nil, err
	}

	var jobAlerts []*job.AlertSpec
	for _, alert := range alerts {
		config, err := job.NewConfig(alert.Config)
		if err != nil {
			return nil, err
		}
		jobAlert, err := job.NewAlertBuilder(job.EventType(alert.On), alert.Channels).
			WithConfig(config).
			Build()
		if err != nil {
			return nil, err
		}
		jobAlerts = append(jobAlerts, jobAlert)
	}

	return jobAlerts, nil
}

func fromStorageAssets(raw []byte) (map[string]string, error) {
	var assetsMap map[string]string
	if err := json.Unmarshal(raw, &assetsMap); err != nil {
		return nil, err
	}
	return assetsMap, nil
}

func FromRow(row pgx.Row) (*Spec, error) {
	var js Spec

	err := row.Scan(&js.ID, &js.Name, &js.Version, &js.Owner, &js.Description,
		&js.Labels, &js.StartDate, &js.EndDate, &js.Interval, &js.DependsOnPast,
		&js.CatchUp, &js.Retry, &js.Alert, &js.StaticUpstreams, &js.HTTPUpstreams,
		&js.TaskName, &js.TaskConfig, &js.WindowSize, &js.WindowOffset, &js.WindowTruncateTo,
		&js.Assets, &js.Hooks, &js.Metadata, &js.Destination, &js.Sources,
		&js.ProjectName, &js.NamespaceName, &js.CreatedAt, &js.UpdatedAt, &js.DeletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job not found")
		}

		return nil, errors.Wrap(resource.EntityResource, "error in reading row for resource", err)
	}

	return &js, nil
}

func UpstreamFromRow(row pgx.Row) (*JobWithUpstream, error) {
	var js JobWithUpstream

	err := row.Scan(&js.JobName, &js.ProjectName, &js.UpstreamJobName, &js.UpstreamResourceURN,
		&js.UpstreamProjectName, &js.UpstreamNamespaceName, &js.UpstreamTaskName, &js.UpstreamHost,
		&js.UpstreamType, &js.UpstreamState, &js.UpstreamExternal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job upstream not found")
		}

		return nil, errors.Wrap(resource.EntityResource, "error in reading row for resource", err)
	}

	return &js, nil
}
