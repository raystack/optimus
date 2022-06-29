package http

import (
	"fmt"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"golang.org/x/net/context"
	"net/http"
	"strings"
)

type optimusRequestConstructor struct {
	managerConfig *config.ResourceManager
	filter        models.JobSpecFilter
}

func constructGetJobSpecsRequest(ctx context.Context, resourceManagerConfig *config.ResourceManager, filter models.JobSpecFilter) (*http.Request, error) {
	if resourceManagerConfig.Type == "optimus" {
		requestConstructor := optimusRequestConstructor{
			managerConfig: resourceManagerConfig,
			filter:        filter,
		}
		return requestConstructor.constructGetJobSpecsRequest(ctx)
	}
	return nil, fmt.Errorf("resource manager type %s is not recognized", resourceManagerConfig.Type)
}

func (o *optimusRequestConstructor) constructGetJobSpecsRequest(ctx context.Context) (*http.Request, error) {
	optimusConfig, ok := o.managerConfig.Config.(resourceManagerOptimusConfig)
	if !ok {
		return nil, fmt.Errorf("config is not valid for resource manager %s with type %s",
			o.managerConfig.Name, o.managerConfig.Type)
	}

	var filters []string
	if o.filter.JobName != "" {
		filters = append(filters, fmt.Sprintf("job_name=%s", o.filter.JobName))
	}
	if o.filter.ProjectName != "" {
		filters = append(filters, fmt.Sprintf("project_name=%s", o.filter.ProjectName))
	}
	if o.filter.ResourceDestination != "" {
		filters = append(filters, fmt.Sprintf("resource_destination=%s", o.filter.ResourceDestination))
	}

	path := "/api/v1beta1/jobs"
	url := optimusConfig.Host + path + strings.Join(filters, "&")

	request, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	return request, nil
}
