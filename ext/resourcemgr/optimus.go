package resourcemgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetJobSpecifications(context.Context, models.JobSpecFilter) ([]models.JobSpec, error)
}

type optimusResourceManager struct {
	optimusConfig *config.ResourceManagerConfigOptimus
	httpClient    *http.Client
}

// NewOptimusResourceManager initializes job spec repository for Optimus neighbor
func NewOptimusResourceManager(config *config.ResourceManagerConfigOptimus) (ResourceManager, error) {
	if config == nil {
		return nil, errors.New("optimus resource manager config is nil")
	}
	return &optimusResourceManager{
		optimusConfig: config,
		httpClient:    http.DefaultClient,
	}, nil
}

func (e *optimusResourceManager) GetJobSpecifications(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	request, err := e.constructGetJobSpecificationsRequest(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("error encountered when constructing request: %w", err)
	}

	response, err := e.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var jobSpecResponse getJobSpecificationsResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobSpecResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return toJobSpecs(jobSpecResponse.Jobs), nil
}

func (e *optimusResourceManager) constructGetJobSpecificationsRequest(ctx context.Context, filter models.JobSpecFilter) (*http.Request, error) {
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
	url := e.optimusConfig.Host + path + strings.Join(filters, "&")

	request, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	return request, nil
}
