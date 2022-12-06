package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
)

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetOptimusUpstreams(ctx context.Context, unresolvedDependency *dto.RawUpstream) ([]*job.Upstream, error)
}

type OptimusResourceManager struct {
	name   string
	config config.ResourceManagerConfigOptimus

	httpClient *http.Client
}

// NewOptimusResourceManager initializes job spec repository for Optimus neighbor
func NewOptimusResourceManager(resourceManagerConfig config.ResourceManager) (*OptimusResourceManager, error) {
	var conf config.ResourceManagerConfigOptimus
	if err := mapstructure.Decode(resourceManagerConfig.Config, &conf); err != nil {
		return nil, fmt.Errorf("error decoding resource manger config: %w", err)
	}
	if conf.Host == "" {
		return nil, errors.New("optimus resource manager host is empty")
	}
	return &OptimusResourceManager{
		name:       resourceManagerConfig.Name,
		config:     conf,
		httpClient: http.DefaultClient,
	}, nil
}

func (o *OptimusResourceManager) GetOptimusUpstreams(ctx context.Context, unresolvedDependency *dto.RawUpstream) ([]*job.Upstream, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	request, err := o.constructGetJobSpecificationsRequest(ctx, unresolvedDependency)
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

	var jobSpecResponse getJobSpecificationsResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobSpecResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return o.toOptimusDependencies(jobSpecResponse.JobSpecificationResponses, unresolvedDependency)
}

func (o *OptimusResourceManager) constructGetJobSpecificationsRequest(ctx context.Context, unresolvedDependency *dto.RawUpstream) (*http.Request, error) {
	var filters []string
	if unresolvedDependency.JobName != "" {
		filters = append(filters, fmt.Sprintf("job_name=%s", unresolvedDependency.JobName))
	}
	if unresolvedDependency.ProjectName != "" {
		filters = append(filters, fmt.Sprintf("project_name=%s", unresolvedDependency.ProjectName))
	}
	if unresolvedDependency.ResourceURN != "" {
		filters = append(filters, fmt.Sprintf("resource_destination=%s", unresolvedDependency.ResourceURN))
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

func (o *OptimusResourceManager) toOptimusDependencies(responses []jobSpecificationResponse, unresolvedDependency *dto.RawUpstream) ([]*job.Upstream, error) {
	output := make([]*job.Upstream, len(responses))
	for i, r := range responses {
		dependency, err := o.toOptimusDependency(r, unresolvedDependency)
		if err != nil {
			return nil, err
		}
		output[i] = dependency
	}
	return output, nil
}

func (o *OptimusResourceManager) toOptimusDependency(response jobSpecificationResponse, unresolvedDependency *dto.RawUpstream) (*job.Upstream, error) {
	jobTenant, err := tenant.NewTenant(response.ProjectName, response.NamespaceName)
	if err != nil {
		return nil, err
	}
	var dependencyType job.UpstreamType
	if unresolvedDependency.IsStatic() {
		dependencyType = job.UpstreamTypeStatic
	} else {
		dependencyType = job.UpstreamTypeInferred
	}
	jobName, err := job.NameFrom(response.Job.Name)
	if err != nil {
		return nil, err
	}
	taskName, err := job.TaskNameFrom(response.Job.TaskName)
	if err != nil {
		return nil, err
	}
	resourceURN := job.ResourceURN(unresolvedDependency.ResourceURN)
	return job.NewUpstreamResolved(jobName, o.config.Host, resourceURN, jobTenant, dependencyType, taskName, true), nil
}
