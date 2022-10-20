package resourcemanager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/models"
)

type OptimusResourceManager struct {
	suite.Suite
}

func (o *OptimusResourceManager) TestGetJobSpecifications() {
	apiPath := "/api/v1beta1/jobs"

	o.Run("should return nil and error if context is nil", func() {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		var ctx context.Context
		var unresolvedDependency models.UnresolvedJobDependency

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
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		var unresolvedDependency models.UnresolvedJobDependency

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
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
		})

		ctx := context.Background()
		var unresolvedDependency models.UnresolvedJobDependency

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
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
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
		var unresolvedDependency models.UnresolvedJobDependency

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
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			expectedHeaderValue := "value"
			actualHeaderValue := r.Header.Get("key")
			o.EqualValues(expectedHeaderValue, actualHeaderValue)

			expectedRawQuery := "job_name=job&project_name=project&resource_destination=resource"
			actualRawQuery := r.URL.RawQuery
			o.EqualValues(expectedRawQuery, actualRawQuery)

			getJobSpecificationResponse := `
{
    "jobSpecificationResponses": [
        {
            "projectName": "project",
            "namespaceName": "namespace",
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
		unresolvedDependency := models.UnresolvedJobDependency{
			ProjectName:         "project",
			JobName:             "job",
			ResourceDestination: "resource",
		}

		expectedJobSpecifications := []models.OptimusDependency{
			{
				Name: "other-optimus",
				Host: server.URL,
				Headers: map[string]string{
					"key": "value",
				},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
			},
		}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.EqualValues(expectedJobSpecifications, actualOptimusDependencies)
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
		manager, err := resourcemgr.NewOptimusResourceManager(conf)
		if err != nil {
			panic(err)
		}

		router.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
			expectedHeaderValue := "value"
			actualHeaderValue := r.Header.Get("key")
			o.EqualValues(expectedHeaderValue, actualHeaderValue)

			expectedRawQuery := "job_name=job&project_name=project&resource_destination=resource"
			actualRawQuery := r.URL.RawQuery
			o.EqualValues(expectedRawQuery, actualRawQuery)

			getJobSpecificationResponse := `
{
    "jobSpecificationResponses": [
        {
            "projectName": "project",
            "namespaceName": "namespace",
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
		unresolvedDependency := models.UnresolvedJobDependency{
			ProjectName:         "project",
			JobName:             "job",
			ResourceDestination: "resource",
		}

		expectedJobSpecifications := []models.OptimusDependency{
			{
				Name: "other-optimus",
				Host: server.URL,
				Headers: map[string]string{
					"key": "value",
				},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
				TaskName:      "task-1",
			},
		}

		actualOptimusDependencies, actualError := manager.GetOptimusDependencies(ctx, unresolvedDependency)

		o.EqualValues(expectedJobSpecifications, actualOptimusDependencies)
		o.NoError(actualError)
	})
}

func TestNewOptimusResourceManager(t *testing.T) {
	t.Run("should return nil and error if config cannot be decoded", func(t *testing.T) {
		var conf config.ResourceManager

		actualResourceManager, actualError := resourcemgr.NewOptimusResourceManager(conf)

		assert.Nil(t, actualResourceManager)
		assert.Error(t, actualError)
	})

	t.Run("should return nil and error if host is empty", func(t *testing.T) {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{},
		}

		actualResourceManager, actualError := resourcemgr.NewOptimusResourceManager(conf)

		assert.Nil(t, actualResourceManager)
		assert.Error(t, actualError)
	})

	t.Run("should return resource manager and nil if no error is encountered", func(t *testing.T) {
		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}

		actualResourceManager, actualError := resourcemgr.NewOptimusResourceManager(conf)

		assert.NotNil(t, actualResourceManager)
		assert.NoError(t, actualError)
	})
}

func TestOptimusResourceManager(t *testing.T) {
	suite.Run(t, &OptimusResourceManager{})
}
