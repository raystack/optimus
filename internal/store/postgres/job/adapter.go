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

	// Dependencies
	StaticDependencies pq.StringArray `gorm:"type:varchar(220)[]" json:"static_dependencies"`

	// ExternalDependencies
	HTTPDependencies datatypes.JSON `json:"http_dependencies"`

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

func toStorageSpec(jobEntity *job.Job) (*Spec, error) {
	jobSpec := jobEntity.Spec()

	labelsBytes, err := json.Marshal(jobSpec.Labels())
	if err != nil {
		return nil, err
	}

	startDate, err := time.Parse(jobDatetimeLayout, jobSpec.Schedule().StartDate())
	if err != nil {
		return nil, err
	}

	var endDate time.Time
	if jobSpec.Schedule().EndDate() != "" {
		endDate, err = time.Parse(jobDatetimeLayout, jobSpec.Schedule().EndDate())
		if err != nil {
			return nil, err
		}
	}

	retryBytes, err := json.Marshal(jobSpec.Schedule().Retry())
	if err != nil {
		return nil, err
	}

	alertsBytes, err := toStorageAlerts(jobSpec.Alerts())
	if err != nil {
		return nil, err
	}

	taskConfigBytes, err := json.Marshal(jobSpec.Task().Config())
	if err != nil {
		return nil, err
	}

	assetsBytes, err := toStorageAsset(jobSpec.Assets())
	if err != nil {
		return nil, err
	}

	hooksBytes, err := toStorageHooks(jobSpec.Hooks())
	if err != nil {
		return nil, err
	}

	metadataBytes, err := json.Marshal(jobSpec.Metadata())
	if err != nil {
		return nil, err
	}

	var staticDependencies []string
	var httpDependenciesBytes []byte
	if jobSpec.Upstream() != nil {
		staticDependencies = jobSpec.Upstream().UpstreamNames()
		httpDependenciesBytes, err = json.Marshal(jobSpec.Upstream().HTTPUpstreams())
		if err != nil {
			return nil, err
		}
	}

	nsName, err := jobSpec.Tenant().NamespaceName()
	if err != nil {
		return nil, err
	}

	sources := make([]string, len(jobEntity.Sources()))
	for i, source := range jobEntity.Sources() {
		sources[i] = source
	}

	return &Spec{
		Name:        jobSpec.Name().String(),
		Version:     jobSpec.Version(),
		Owner:       jobSpec.Owner(),
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

		StaticDependencies: staticDependencies,
		HTTPDependencies:   httpDependenciesBytes,

		Destination: jobEntity.Destination(),
		Sources:     sources,

		NamespaceName: nsName.String(),
		ProjectName:   jobSpec.Tenant().ProjectName().String(),
	}, nil
}

func toStorageHooks(hookSpecs []*job.Hook) ([]byte, error) {
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
	configJSON, err := json.Marshal(spec.Config().Config())
	if err != nil {
		return Hook{}, err
	}
	return Hook{
		Name:   spec.Name(),
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
	var alerts []Alert
	for _, alertSpec := range alertSpecs {
		alerts = append(alerts, Alert{
			On:       string(alertSpec.On()),
			Config:   alertSpec.Config(),
			Channels: alertSpec.Channels(),
		})
	}
	return json.Marshal(alerts)
}
