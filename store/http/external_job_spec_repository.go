package http

import (
	"context"
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
	request, err := constructGetJobSpecsRequest(ctx, e.resourceManagerConfig, filter)
	if err != nil {
		return nil, err
	}

	response, err := e.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	return nil, nil
}

type resourceManagerOptimusConfig struct {
	Host string
	// TODO: Add header
}
