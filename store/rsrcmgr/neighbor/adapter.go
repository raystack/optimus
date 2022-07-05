package neighbor

import (
	"github.com/odpf/optimus/models"
)

type getJobSpecificationsResponse struct {
	Jobs []jobSpecificationResponse `json:"jobs"`
}

type jobSpecificationResponse struct {
	ProjectName   string           `json:"projectName"`
	NamespaceName string           `json:"namespaceName"`
	Job           jobSpecification `json:"job"`
}

type jobSpecification struct {
	Version          int                      `json:"version"`
	Name             string                   `json:"name"`
	Owner            string                   `json:"owner"`
	StartDate        string                   `json:"startDate"`
	EndDate          string                   `json:"endDate"`
	Interval         string                   `json:"interval"`
	DependsOnPast    string                   `json:"dependsOnPast"`
	CatchUp          string                   `json:"catchUp"`
	TaskName         string                   `json:"taskName"`
	Config           []jobConfigItem          `json:"config"`
	WindowSize       string                   `json:"windowSize"`
	WindowOffset     string                   `json:"windowOffset"`
	WindowTruncateTo string                   `json:"windowTruncateTo"`
	Dependencies     []jobDependency          `json:"dependencies"`
	Assets           map[string]string        `json:"assets"`
	Hooks            []jobSpecHook            `json:"hooks"`
	Description      string                   `json:"description"`
	Labels           map[string]string        `json:"labels"`
	Behavior         jobSpecificationBehavior `json:"behavior"`
	Metadata         jobMetadata              `json:"metadata"`
}

type jobMetadata struct {
	Resource jobSpecMetadataResource `json:"resource"`
	Airflow  jobSpecMetadataAirflow  `json:"airflow"`
}

type jobSpecMetadataAirflow struct {
	Pool  string `json:"pool"`
	Queue string `json:"queue"`
}

type jobSpecMetadataResource struct {
	Request jobSpecMetadataResourceConfig `json:"request"`
	Limit   jobSpecMetadataResourceConfig `json:"limit"`
}

type jobSpecMetadataResourceConfig struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

type jobSpecificationBehavior struct {
	Retry  behaviorRetry       `json:"retry"`
	Notify []behaviorNotifiers `json:"notify"`
}

type behaviorNotifiers struct {
	On       jobEventType      `json:"on"`
	Channels []string          `json:"channels"`
	Config   map[string]string `json:"config"`
}

type jobEventType string

type behaviorRetry struct {
	Count              int    `json:"count"`
	Delay              string `json:"delay"`
	ExponentialBackoff bool   `json:"exponentialBackoff"`
}

type jobSpecHook struct {
	Name   string        `json:"name"`
	Config jobConfigItem `json:"config"`
}

type jobDependency struct {
	Name           string         `json:"name"`
	Type           string         `json:"type"`
	HTTPDependency httpDependency `json:"httpDependency"`
}

type httpDependency struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Params  map[string]string `json:"params"`
}

type jobConfigItem struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func toJobSpecs(responses []jobSpecificationResponse) []models.JobSpec {
	output := make([]models.JobSpec, len(responses))
	for i, r := range responses {
		output[i] = toJobSpec(r)
	}
	return output
}

func toJobSpec(response jobSpecificationResponse) models.JobSpec {
	return models.JobSpec{
		Name: response.Job.Name,
		NamespaceSpec: models.NamespaceSpec{
			Name: response.NamespaceName,
			ProjectSpec: models.ProjectSpec{
				Name: response.ProjectName,
			},
		},
	}
}
