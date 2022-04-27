package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

func TestPluginService(t *testing.T) {
	ctx := context.Background()
	depMod := new(mock.DependencyResolverMod)
	baseUnit := new(mock.BasePlugin)
	plugin := &models.Plugin{Base: baseUnit, DependencyMod: depMod}
	baseUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: "bq",
	}, nil)

	projectName := "a-data-project"
	projectSpec := models.ProjectSpec{
		Name: projectName,
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		Name: "namespace-123",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: projectSpec,
	}

	secrets := []models.ProjectSecretItem{
		{
			ID:    uuid.New(),
			Name:  "table_name",
			Value: "secret_table",
			Type:  models.SecretTypeUserDefined,
		},
		{
			ID:    uuid.New(),
			Name:  "bucket",
			Value: "gs://some_secret_bucket",
			Type:  models.SecretTypeUserDefined,
		},
	}
	engine := compiler.NewGoEngine()

	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("return error when not able to get plugin", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{}, errors.New("plugin not found"))
			defer pluginRepo.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(nil, pluginRepo, nil)
			resp, err := pluginService.GenerateDestination(ctx, jobSpec, namespaceSpec)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "plugin not found")
		})
		t.Run("return err when not no dependency mod in plugin", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{Base: baseUnit}, nil)
			defer pluginRepo.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(nil, pluginRepo, nil)
			resp, err := pluginService.GenerateDestination(ctx, jobSpec, namespaceSpec)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "dependency mod not found for plugin")
		})
		t.Run("return error when not not able to compile configs", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)
			defer pluginRepo.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return([]models.ProjectSecretItem{}, errors.New("error"))
			defer secretService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(secretService, pluginRepo, engine)
			resp, err := pluginService.GenerateDestination(ctx, jobSpec, namespaceSpec)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "error")
		})
		t.Run("return destination successfully", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)
			defer pluginRepo.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return(secrets, nil)
			defer secretService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "SECRET_TABLE_NAME",
							Value: "{{.secret.table_name}}",
						},
						{
							Name:  "DEND",
							Value: `? DATE(event_timestamp) >= "{{ .DSTART|DATE }}" AND DATE(event_timestamp)< "{{ .DEND|DATE }}"`,
						},
					},
				},
			}

			destReq := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{
					{
						Name:  "SECRET_TABLE_NAME",
						Value: "secret_table",
					},
					{
						Name:  "DEND",
						Value: `? DATE(event_timestamp) >= "{{ .DSTART|DATE }}" AND DATE(event_timestamp)< "{{ .DEND|DATE }}"`,
					},
				},
				Assets:  models.PluginAssets{},
				Project: namespaceSpec.ProjectSpec,
			}
			depMod.On("GenerateDestination", ctx, destReq).Return(&models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}, nil)

			pluginService := service.NewPluginService(secretService, pluginRepo, engine)
			resp, err := pluginService.GenerateDestination(ctx, jobSpec, namespaceSpec)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, "project.dataset.table", resp.Destination)
			assert.Equal(t, models.DestinationTypeBigquery, resp.Type)
		})
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		t.Run("return error when not able to get plugin", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{}, errors.New("plugin not found"))
			defer pluginRepo.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(nil, pluginRepo, nil)
			resp, err := pluginService.GenerateDependencies(ctx, jobSpec, namespaceSpec, false)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "plugin not found")
		})
		t.Run("return err when no dependency mod in plugin", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{Base: baseUnit}, nil)
			defer pluginRepo.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(nil, pluginRepo, nil)
			resp, err := pluginService.GenerateDependencies(ctx, jobSpec, namespaceSpec, false)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "dependency mod not found for plugin")
		})
		t.Run("return error when not not able to compile configs", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)
			defer pluginRepo.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return([]models.ProjectSecretItem{}, errors.New("error"))
			defer secretService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
			}

			pluginService := service.NewPluginService(secretService, pluginRepo, engine)
			resp, err := pluginService.GenerateDependencies(ctx, jobSpec, namespaceSpec, false)
			assert.Nil(t, resp)
			assert.EqualError(t, err, "error")
		})
		t.Run("return dependencies successfully", func(t *testing.T) {
			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)
			defer pluginRepo.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return(secrets, nil)
			defer secretService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Task: models.JobSpecTask{
					Unit: plugin,
					Config: models.JobSpecConfigs{
						{
							Name:  "SECRET_TABLE_NAME",
							Value: "{{.secret.table_name}}",
						},
					},
				},
			}

			destReq := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{
					{
						Name:  "SECRET_TABLE_NAME",
						Value: "secret_table",
					},
				},
				Assets:  models.PluginAssets{},
				Project: namespaceSpec.ProjectSpec,
			}
			depMod.On("GenerateDependencies", ctx, destReq).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"}},
				nil)

			pluginService := service.NewPluginService(secretService, pluginRepo, engine)
			resp, err := pluginService.GenerateDependencies(ctx, jobSpec, namespaceSpec, false)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, "project.dataset.table2_destination", resp.Dependencies[0])
		})
	})
}
