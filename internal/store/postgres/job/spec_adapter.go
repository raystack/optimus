package job

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
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
	StaticDependencies []string

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
	Sources     []string

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
	jobSpec := jobEntity.JobSpec()

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

	httpDependenciesBytes, err := json.Marshal(jobSpec.Dependencies().HttpDependencies())
	if err != nil {
		return nil, err
	}

	nsName := ""
	if ns, err := jobSpec.Tenant().Namespace(); err == nil {
		nsName = ns.Name().String()
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

		StaticDependencies: jobSpec.Dependencies().JobDependencies(),
		HTTPDependencies:   httpDependenciesBytes,

		Destination: jobEntity.Destination(),
		Sources:     sources,

		NamespaceName: nsName,
		ProjectName:   jobSpec.Tenant().Project().Name().String(),
	}, nil
}

func toStorageHooks(hookSpecs []*dto.Hook) ([]byte, error) {
	hooks := make([]Hook, len(hookSpecs))
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

func toStorageHook(spec *dto.Hook) (Hook, error) {
	configJSON, err := json.Marshal(spec.Config)
	if err != nil {
		return Hook{}, err
	}
	return Hook{
		Name:   spec.Name(),
		Config: configJSON,
	}, nil
}

func toStorageAsset(assetSpecs map[string]string) ([]byte, error) {
	assets := make([]Asset, len(assetSpecs))
	for key, val := range assetSpecs {
		assets = append(assets, Asset{Name: key, Value: val})
	}
	assetsJSON, err := json.Marshal(assets)
	if err != nil {
		return nil, err
	}
	return assetsJSON, nil
}

func toStorageAlerts(alertSpecs []*dto.Alert) ([]byte, error) {
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
