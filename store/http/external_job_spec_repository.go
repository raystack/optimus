package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type externalJobSpecRepository struct {
	resourceManagerConfig *config.ResourceManager
	httpClient            *http.Client
}

// NewExternalJobSpecRepository initializes external job spec repository
func NewExternalJobSpecRepository(resourceManagerConfig *config.ResourceManager) (store.ExternalJobSpecRepository, error) {
	if resourceManagerConfig == nil {
		return nil, errors.New("resource manager config is nil")
	}
	return &externalJobSpecRepository{
		resourceManagerConfig: resourceManagerConfig,
		httpClient:            http.DefaultClient,
	}, nil
}

func (e *externalJobSpecRepository) GetJobSpecifications(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	request, err := constructGetJobSpecificationsRequest(ctx, e.resourceManagerConfig, filter)
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
