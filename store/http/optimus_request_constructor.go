package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type resourceManagerOptimusConfig struct {
	Host string
	// TODO: Add header
}

type optimusRequestConstructor struct {
	managerConfig *config.ResourceManager
	filter        models.JobSpecFilter
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
