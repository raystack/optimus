package setup

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/internal/models"
)

type MockPluginBQ struct{}

func (MockPluginBQ) GetName(_ context.Context) (string, error) {
	return "bq2bq", nil
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

func (MockPluginBQ) CompileAssets(_ context.Context, _ models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	// TODO: implement mock
	return &models.CompileAssetsResponse{}, nil
}

//func InMemoryPluginRegistry() models.PluginRepository {
//	bq2bq := MockPluginBQ{}
//
//	transporterHook := "transporter"
//	hookUnit := new(mock.YamlMod)
//	hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
//		Name:       transporterHook,
//		HookType:   models.HookTypePre,
//		PluginType: models.PluginTypeHook,
//		Image:      "example.io/namespace/hook-image:latest",
//	}, nil)
//
//	pluginRepo := new(mock.PluginRepository)
//	pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
//		YamlMod:       hookUnit,
//		DependencyMod: bq2bq,
//	}, nil)
//	pluginRepo.On("GetByName", "transporter").Return(&models.Plugin{
//		YamlMod: hookUnit,
//	}, nil)
//	return pluginRepo
//}
