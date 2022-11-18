package resourcemanager_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/resourcemanager"
)

type OptimusResourceManager struct {
	suite.Suite
}

func (o *OptimusResourceManager) TestGetJobSpecifications() {
	apiPath := "/api/v1beta1/jobs"
	sampleTenant, _ := tenant.NewTenant("test-proj", "test-ns")

	o.Run("should return nil and error if context is nil", func() {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		var ctx context.Context
		var unresolvedDependency *dto.RawUpstream

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.Nil(actualOptimusDependencies)
		o.Error(actualError)
	})

	o.Run("should return nil and error if error is encountered when creating request", func() {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: ":invalid-url",
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		unresolvedDependency := &dto.RawUpstream{
			ProjectName: "test-proj",
			JobName:     "job",
			ResourceURN: "resource",
		}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.Nil(actualOptimusDependencies)
		o.Error(actualError)
	})

	o.Run("should return nil and error if http response is not ok", func() {
		router := mux.NewRouter()
		server := httptest.NewServer(router)
		defer server.Close()

		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: server.URL,
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
		})

		ctx := context.Background()
		unresolvedDependency := &dto.RawUpstream{
			ProjectName: "test-proj",
			JobName:     "job",
			ResourceURN: "resource",
		}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.Nil(actualOptimusDependencies)
		o.Error(actualError)
	})

	o.Run("should return nil and error if cannot decode response into proper response type", func() {
		router := mux.NewRouter()
		server := httptest.NewServer(router)
		defer server.Close()

		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: server.URL,
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			content := []byte("invalid-content")

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})

		ctx := context.Background()
		unresolvedDependency := &dto.RawUpstream{
			ProjectName: "test-proj",
			JobName:     "job",
			ResourceURN: "resource",
		}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.Nil(actualOptimusDependencies)
		o.Error(actualError)
	})

	o.Run("should return job specifications and nil if no error is encountered", func() {
		router := mux.NewRouter()
		server := httptest.NewServer(router)
		defer server.Close()

		conf := config.ResourceManager{
			Name: "other-optimus",
			Config: config.ResourceManagerConfigOptimus{
				Host: server.URL,
				Headers: map[string]string{
					"key": "value",
				},
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			expectedHeaderValue := "value"
			actualHeaderValue := r.Header.Get("key")
			o.EqualValues(expectedHeaderValue, actualHeaderValue)

			expectedRawQuery := "job_name=job&project_name=test-proj&resource_destination=resource"
			actualRawQuery := r.URL.RawQuery
			o.EqualValues(expectedRawQuery, actualRawQuery)

			getJobSpecificationResponse := `
{
    "jobSpecificationResponses": [
        {
            "projectName": "test-proj",
            "namespaceName": "test-ns",
            "job": {
                "version": 0,
                "name": "job"
            }
        }
    ]
}`
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(getJobSpecificationResponse))
		})

		ctx := context.Background()
		unresolvedDependency := &dto.RawUpstream{
			ProjectName: "test-proj",
			JobName:     "job",
			ResourceURN: "resource",
		}

		dependency, _ := job.NewUpstreamResolved("job", server.URL, "resource", sampleTenant, "static")
		expectedDependencies := []*job.Upstream{dependency}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.EqualValues(expectedDependencies, actualOptimusDependencies)
		o.NoError(actualError)
	})

	o.Run("should return job specifications with hook and nil if no error is encountered", func() {
		router := mux.NewRouter()
		server := httptest.NewServer(router)
		defer server.Close()

		conf := config.ResourceManager{
			Name: "other-optimus",
			Config: config.ResourceManagerConfigOptimus{
				Host: server.URL,
				Headers: map[string]string{
					"key": "value",
				},
			},
		}
		manager, err := resourcemanager.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			expectedHeaderValue := "value"
			actualHeaderValue := r.Header.Get("key")
			o.EqualValues(expectedHeaderValue, actualHeaderValue)

			expectedRawQuery := "job_name=job&project_name=test-proj&resource_destination=resource"
			actualRawQuery := r.URL.RawQuery
			o.EqualValues(expectedRawQuery, actualRawQuery)

			getJobSpecificationResponse := `
{
    "jobSpecificationResponses": [
        {
            "projectName": "test-proj",
            "namespaceName": "test-ns",
            "job": {
                "version": 0,
                "name": "job",
                "hooks": [{
                    "name": "hook-1",
                    "config": [{
                        "name": "hook-1-config-1-key",
                        "value": "hook-1-config-1-value"
                    }]
                }],
                "taskName": "task-1"
            }
        }
    ]
}`
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(getJobSpecificationResponse))
		})

		ctx := context.Background()
		unresolvedDependency := &dto.RawUpstream{
			ProjectName: "test-proj",
			JobName:     "job",
			ResourceURN: "resource",
		}

		dependency, _ := job.NewUpstreamResolved("job", server.URL, "resource", sampleTenant, "static")
		expectedDependencies := []*job.Upstream{dependency}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.EqualValues(expectedDependencies, actualOptimusDependencies)
		o.NoError(actualError)
	})
}

func TestNewOptimusResourceManager(t *testing.T) {
	t.Run("should return nil and error if config cannot be decoded", func(t *testing.T) {
		var conf config.ResourceManager

		actualResourceManager, actualError := resourcemanager.NewOptimusResourceManager(conf)

		assert.Nil(t, actualResourceManager)
		assert.Error(t, actualError)
	})

	t.Run("should return nil and error if host is empty", func(t *testing.T) {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{},
		}

		actualResourceManager, actualError := resourcemanager.NewOptimusResourceManager(conf)

		assert.Nil(t, actualResourceManager)
		assert.Error(t, actualError)
	})

	t.Run("should return resource manager and nil if no error is encountered", func(t *testing.T) {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}

		actualResourceManager, actualError := resourcemanager.NewOptimusResourceManager(conf)

		assert.NotNil(t, actualResourceManager)
		assert.NoError(t, actualError)
	})
}

func TestOptimusResourceManager(t *testing.T) {
	suite.Run(t, &OptimusResourceManager{})
}