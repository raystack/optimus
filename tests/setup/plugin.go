package setup

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/sdk/plugin"
)

type MockPluginBQ struct{}

func (MockPluginBQ) GetName(_ context.Context) (string, error) {
	return "bq2bq", nil
}

func (MockPluginBQ) GenerateDestination(_ context.Context, request plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &plugin.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        "bigquery",
		}, nil
	}
	return nil, errors.New("missing config key required to generate destination")
}

func (MockPluginBQ) GenerateDependencies(_ context.Context, req plugin.GenerateDependenciesRequest) (*plugin.GenerateDependenciesResponse, error) {
	c, _ := req.Config.Get("DEST")
	return &plugin.GenerateDependenciesResponse{Dependencies: []string{c.Value}}, nil
}

func (MockPluginBQ) CompileAssets(_ context.Context, _ plugin.CompileAssetsRequest) (*plugin.CompileAssetsResponse, error) {
	// TODO: implement mock
	return &plugin.CompileAssetsResponse{}, nil
}
