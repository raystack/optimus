package setup

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type MockPluginBQ struct{}

func (MockPluginBQ) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          "bq2bq",
		Description:   "BigQuery to BigQuery transformation task",
		PluginType:    models.PluginTypeTask,
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []models.PluginMod{models.ModTypeDependencyResolver},
	}, nil
}

func (MockPluginBQ) GenerateDestination(_ context.Context, request models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &models.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        models.DestinationTypeBigquery,
		}, nil
	}
	return nil, errors.New("missing config key required to generate destination")
}

func (MockPluginBQ) GenerateDependencies(_ context.Context, req models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	c, _ := req.Config.Get("DEST")
	return &models.GenerateDependenciesResponse{Dependencies: []string{c.Value}}, nil
}

func InMemoryPluginRegistry() models.PluginRepository {
	bq2bq := MockPluginBQ{}

	transporterHook := "transporter"
	hookUnit := new(mock.BasePlugin)
	hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       transporterHook,
		HookType:   models.HookTypePre,
		PluginType: models.PluginTypeHook,
		Image:      "example.io/namespace/hook-image:latest",
	}, nil)

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
		Base:          bq2bq,
		DependencyMod: bq2bq,
	}, nil)
	pluginRepo.On("GetByName", "transporter").Return(&models.Plugin{
		Base: hookUnit,
	}, nil)
	return pluginRepo
}
