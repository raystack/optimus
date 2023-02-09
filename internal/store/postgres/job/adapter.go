package job

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
)

const jobDatetimeLayout = "2006-01-02"

type Spec struct {
	ID          uuid.UUID
	Name        string
	Version     int
	Owner       string
	Description string
	Labels      map[string]string

	Schedule   json.RawMessage
	WindowSpec json.RawMessage

	Alert json.RawMessage

	StaticUpstreams pq.StringArray
	HTTPUpstreams   json.RawMessage

	TaskName   string
	TaskConfig map[string]string

	Hooks json.RawMessage

	Assets map[string]string

	Metadata json.RawMessage

	Destination string
	Sources     pq.StringArray

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime
}

type Schedule struct {
	StartDate     time.Time
	EndDate       *time.Time `json:",omitempty"`
	Interval      string
	DependsOnPast bool
	CatchUp       bool
	Retry         *Retry
}

type Window struct {
	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string
}

type Retry struct {
	Count              int
	Delay              int32
	ExponentialBackoff bool
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
	Config map[string]string
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

	alertsBytes, err := toStorageAlerts(jobSpec.AlertSpecs())
	if err != nil {
		return nil, err
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

	var assets map[string]string
	if jobSpec.Asset() != nil {
		assets = jobSpec.Asset()
	}

	schedule, err := toStorageSchedule(jobSpec.Schedule())
	if err != nil {
		return nil, err
	}

	windowBytes, err := toStorageWindow(jobSpec.Window())
	if err != nil {
		return nil, err
	}

	return &Spec{
		Name:        jobSpec.Name().String(),
		Version:     jobSpec.Version(),
		Owner:       jobSpec.Owner(),
		Description: jobSpec.Description(),
		Labels:      jobSpec.Labels(),
		Assets:      assets,
		Metadata:    metadataBytes,

		Schedule:   schedule,
		WindowSpec: windowBytes,

		Alert: alertsBytes,

		TaskName:   jobSpec.Task().Name().String(),
		TaskConfig: jobSpec.Task().Config(),

		Hooks: hooksBytes,

		StaticUpstreams: staticUpstreams,
		HTTPUpstreams:   httpUpstreamsInBytes,

		Destination: jobEntity.Destination().String(),
		Sources:     sources,

		ProjectName:   jobEntity.Tenant().ProjectName().String(),
		NamespaceName: jobEntity.Tenant().NamespaceName().String(),
	}, nil
}

func toStorageWindow(windowSpec models.Window) ([]byte, error) {
	window := Window{
		WindowSize:       windowSpec.GetSize(),
		WindowOffset:     windowSpec.GetOffset(),
		WindowTruncateTo: windowSpec.GetTruncateTo(),
	}
	windowJSON, err := json.Marshal(window)
	if err != nil {
		return nil, err
	}
	return windowJSON, nil
}

func toStorageHooks(hookSpecs []*job.Hook) ([]byte, error) {
	if hookSpecs == nil {
		return nil, nil
	}
	var hooks []Hook
	for _, hookSpec := range hookSpecs {
		hook := toStorageHook(hookSpec)
		hooks = append(hooks, hook)
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return nil, err
	}
	return hooksJSON, nil
}

func toStorageHook(spec *job.Hook) Hook {
	return Hook{
		Name:   spec.Name(),
		Config: spec.Config(),
	}
}

func toStorageAlerts(alertSpecs []*job.AlertSpec) ([]byte, error) {
	if alertSpecs == nil {
		return nil, nil
	}
	var alerts []Alert
	for _, alertSpec := range alertSpecs {
		alerts = append(alerts, Alert{
			On:       alertSpec.On(),
			Config:   alertSpec.Config(),
			Channels: alertSpec.Channels(),
		})
	}
	return json.Marshal(alerts)
}

func toStorageSchedule(scheduleSpec *job.Schedule) ([]byte, error) {
	if scheduleSpec == nil {
		return nil, nil
	}

	startDate, err := time.Parse(jobDatetimeLayout, scheduleSpec.StartDate().String())
	if err != nil {
		return nil, err
	}

	var retry *Retry
	if scheduleSpec.Retry() != nil {
		retry = &Retry{
			Count:              scheduleSpec.Retry().Count(),
			Delay:              scheduleSpec.Retry().Delay(),
			ExponentialBackoff: scheduleSpec.Retry().ExponentialBackoff(),
		}
	}

	schedule := Schedule{
		StartDate:     startDate,
		Interval:      scheduleSpec.Interval(),
		DependsOnPast: scheduleSpec.DependsOnPast(),
		CatchUp:       scheduleSpec.CatchUp(),
		Retry:         retry,
	}
	if scheduleSpec.EndDate() != "" {
		endDate, err := time.Parse(jobDatetimeLayout, scheduleSpec.EndDate().String())
		if err != nil {
			return nil, err
		}
		schedule.EndDate = &endDate
	}
	return json.Marshal(schedule)
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

func fromStorageSpec(jobSpec *Spec) (*job.Spec, error) {
	version := jobSpec.Version

	jobName, err := job.NameFrom(jobSpec.Name)
	if err != nil {
		return nil, err
	}

	owner := jobSpec.Owner

	var schedule *job.Schedule
	if jobSpec.Schedule != nil {
		schedule, err = fromStorageSchedule(jobSpec.Schedule)
		if err != nil {
			return nil, err
		}
	}

	var window models.Window
	if jobSpec.WindowSpec != nil {
		window, err = fromStorageWindow(jobSpec.WindowSpec, jobSpec.Version)
		if err != nil {
			return nil, err
		}
	}

	var taskConfig job.Config
	if jobSpec.TaskConfig != nil {
		taskConfig, err = job.ConfigFrom(jobSpec.TaskConfig)
		if err != nil {
			return nil, err
		}
	}
	taskName, err := job.TaskNameFrom(jobSpec.TaskName)
	if err != nil {
		return nil, err
	}
	task := job.NewTask(taskName, taskConfig)

	jobSpecBuilder := job.NewSpecBuilder(version, jobName, owner, schedule, window, task).WithDescription(jobSpec.Description)

	if jobSpec.Labels != nil {
		jobSpecBuilder = jobSpecBuilder.WithLabels(jobSpec.Labels)
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
		asset, err := job.AssetFrom(jobSpec.Assets)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithAsset(asset)
	}

	return jobSpecBuilder.Build()
}

func fromStorageWindow(raw []byte, jobVersion int) (models.Window, error) {
	var storageWindow Window
	if err := json.Unmarshal(raw, &storageWindow); err != nil {
		return nil, err
	}

	return models.NewWindow(
		jobVersion,
		storageWindow.WindowTruncateTo,
		storageWindow.WindowOffset,
		storageWindow.WindowSize,
	)
}

func fromStorageSchedule(raw []byte) (*job.Schedule, error) {
	var storageSchedule Schedule
	if err := json.Unmarshal(raw, &storageSchedule); err != nil {
		return nil, err
	}
	startDate, err := job.ScheduleDateFrom(storageSchedule.StartDate.Format(job.DateLayout))
	if err != nil {
		return nil, err
	}
	scheduleBuilder := job.NewScheduleBuilder(startDate).
		WithCatchUp(storageSchedule.CatchUp).
		WithDependsOnPast(storageSchedule.DependsOnPast).
		WithInterval(storageSchedule.Interval)

	if storageSchedule.EndDate != nil && !storageSchedule.EndDate.IsZero() {
		endDate, err := job.ScheduleDateFrom(storageSchedule.EndDate.Format(job.DateLayout))
		if err != nil {
			return nil, err
		}
		scheduleBuilder = scheduleBuilder.WithEndDate(endDate)
	}

	if storageSchedule.Retry != nil {
		retry := job.NewRetry(storageSchedule.Retry.Count, storageSchedule.Retry.Delay, storageSchedule.Retry.ExponentialBackoff)
		scheduleBuilder = scheduleBuilder.WithRetry(retry)
	}

	return scheduleBuilder.Build()
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
	config, err := job.ConfigFrom(hook.Config)
	if err != nil {
		return nil, err
	}
	return job.NewHook(hook.Name, config)
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
		config, err := job.ConfigFrom(alert.Config)
		if err != nil {
			return nil, err
		}
		jobAlert, err := job.NewAlertSpec(alert.On, alert.Channels, config)
		if err != nil {
			return nil, err
		}
		jobAlerts = append(jobAlerts, jobAlert)
	}

	return jobAlerts, nil
}

func FromRow(row pgx.Row) (*Spec, error) {
	var js Spec

	err := row.Scan(&js.ID, &js.Name, &js.Version, &js.Owner, &js.Description,
		&js.Labels, &js.Schedule, &js.Alert, &js.StaticUpstreams, &js.HTTPUpstreams,
		&js.TaskName, &js.TaskConfig, &js.WindowSpec, &js.Assets, &js.Hooks, &js.Metadata, &js.Destination, &js.Sources,
		&js.ProjectName, &js.NamespaceName, &js.CreatedAt, &js.UpdatedAt, &js.DeletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job not found")
		}

		return nil, errors.Wrap(job.EntityJob, "error in reading row for job", err)
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

		return nil, errors.Wrap(job.EntityJob, "error in reading row for upstream", err)
	}

	return &js, nil
}
