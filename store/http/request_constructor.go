package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func constructGetJobSpecificationsRequest(ctx context.Context, resourceManagerConfig *config.ResourceManager, filter models.JobSpecFilter) (*http.Request, error) {
	if resourceManagerConfig.Type == "optimus" {
		requestConstructor := optimusRequestConstructor{
			managerConfig: resourceManagerConfig,
			filter:        filter,
		}
		return requestConstructor.constructGetJobSpecsRequest(ctx)
	}
	return nil, fmt.Errorf("resource manager type %s is not recognized", resourceManagerConfig.Type)
}
