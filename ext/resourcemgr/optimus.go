package resourcemgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetOptimusDependencies(context.Context, models.JobSpecFilter) ([]models.OptimusDependency, error)
}

type optimusResourceManager struct {
	name   string
	config config.ResourceManagerConfigOptimus

	httpClient *http.Client
}

// NewOptimusResourceManager initializes job spec repository for Optimus neighbor
func NewOptimusResourceManager(resourceManagerConfig config.ResourceManager) (ResourceManager, error) {
	var conf config.ResourceManagerConfigOptimus
	if err := mapstructure.Decode(resourceManagerConfig.Config, &conf); err != nil {
		return nil, fmt.Errorf("error decoding resource manger config: %w", err)
	}
	if conf.Host == "" {
		return nil, errors.New("optimus resource manager host is empty")
	}
	return &optimusResourceManager{
		name:       resourceManagerConfig.Name,
		config:     conf,
		httpClient: http.DefaultClient,
	}, nil
}

func (o *optimusResourceManager) GetOptimusDependencies(ctx context.Context, filter models.JobSpecFilter) ([]models.OptimusDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	request, err := o.constructGetJobSpecificationsRequest(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("error encountered when constructing request: %w", err)
	}

	response, err := o.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var jobSpecResponse GetJobSpecificationsResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobSpecResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return o.toOptimusDependencies(jobSpecResponse.JobSpecificationResponses), nil
}

func (o *optimusResourceManager) constructGetJobSpecificationsRequest(ctx context.Context, filter models.JobSpecFilter) (*http.Request, error) {
	var filters []string
	if filter.JobName != "" {
		filters = append(filters, fmt.Sprintf("job_name=%s", filter.JobName))
	}
	if filter.ProjectName != "" {
		filters = append(filters, fmt.Sprintf("project_name=%s", filter.ProjectName))
	}
	if filter.ResourceDestination != "" {
		filters = append(filters, fmt.Sprintf("resource_destination=%s", filter.ResourceDestination))
	}

	path := "/api/v1beta1/jobs"
	url := o.config.Host + path + "?" + strings.Join(filters, "&")

	request, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	for key, value := range o.config.Headers {
		request.Header.Set(key, value)
	}
	return request, nil
}

func (o *optimusResourceManager) toOptimusDependencies(responses []JobSpecificationResponse) []models.OptimusDependency {
	output := make([]models.OptimusDependency, len(responses))
	for i, r := range responses {
		output[i] = o.toOptimusDependency(r)
	}
	return output
}

func (o *optimusResourceManager) toOptimusDependency(response JobSpecificationResponse) models.OptimusDependency {
	return models.OptimusDependency{
		Name:          o.name,
		Host:          o.config.Host,
		Headers:       o.config.Headers,
		ProjectName:   response.ProjectName,
		NamespaceName: response.NamespaceName,
		JobName:       response.Job.Name,
	}
}
