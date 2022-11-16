package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
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

		TaskName:   jobSpec.Name().String(),
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
