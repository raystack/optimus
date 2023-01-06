package plugin

import (
	"context"
	"fmt"
	"time"
)

// DependencyResolverMod needs to be implemented for automatic dependency resolution of tasks
type DependencyResolverMod interface {
	// GetName returns name of the plugin
	GetName(context.Context) (string, error)

	// GenerateDestination derive destination from config and assets
	GenerateDestination(context.Context, GenerateDestinationRequest) (*GenerateDestinationResponse, error)

	// GenerateDependencies returns names of job destination on which this unit is dependent on
	GenerateDependencies(context.Context, GenerateDependenciesRequest) (*GenerateDependenciesResponse, error)

	// CompileAssets overrides default asset compilation
	CompileAssets(context.Context, CompileAssetsRequest) (*CompileAssetsResponse, error)
}

type JobRunSpecData struct {
	Name  string
	Value string
	Type  string
}

type CompileAssetsRequest struct {
	Options

	// Task configs
	Config Configs

	// Job assets
	Assets Assets

	// the instance for which these assets are being compiled for
	InstanceData []JobRunSpecData
	StartTime    time.Time
	EndTime      time.Time
}

type CompileAssetsResponse struct {
	Assets Assets
}

type GenerateDestinationRequest struct {
	// Task configs
	Config Configs

	// Job assets
	Assets Assets

	Options
}

type GenerateDestinationResponse struct {
	Destination string
	Type        string
}

func (gdr GenerateDestinationResponse) URN() string {
	return fmt.Sprintf(DestinationURNFormat, gdr.Type, gdr.Destination)
}

type GenerateDependenciesRequest struct {
	// Task configs
	Config Configs

	// Job assets
	Assets Assets

	Options
}

type GenerateDependenciesResponse struct {
	Dependencies []string
}
