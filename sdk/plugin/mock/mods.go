package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/sdk/plugin"
)

type YamlMod struct {
	mock.Mock `hash:"-"`
}

func (repo *YamlMod) PluginInfo() *plugin.Info {
	args := repo.Called()
	return args.Get(0).(*plugin.Info)
}

func (repo *YamlMod) DefaultConfig(ctx context.Context, inp plugin.DefaultConfigRequest) (*plugin.DefaultConfigResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.DefaultConfigResponse), args.Error(1)
}

func (repo *YamlMod) DefaultAssets(ctx context.Context, inp plugin.DefaultAssetsRequest) (*plugin.DefaultAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.DefaultAssetsResponse), args.Error(1)
}

func (repo *YamlMod) GetQuestions(ctx context.Context, inp plugin.GetQuestionsRequest) (*plugin.GetQuestionsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.GetQuestionsResponse), args.Error(1)
}

func (repo *YamlMod) ValidateQuestion(ctx context.Context, inp plugin.ValidateQuestionRequest) (*plugin.ValidateQuestionResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.ValidateQuestionResponse), args.Error(1)
}

type DependencyResolverMod struct {
	mock.Mock `hash:"-"`
}

func (repo *DependencyResolverMod) GetName(ctx context.Context) (string, error) {
	args := repo.Called(ctx)
	return args.Get(0).(string), args.Error(1)
}

func (repo *DependencyResolverMod) GenerateDestination(ctx context.Context, inp plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.GenerateDestinationResponse), args.Error(1)
}

func (repo *DependencyResolverMod) GenerateDependencies(ctx context.Context, inp plugin.GenerateDependenciesRequest) (*plugin.GenerateDependenciesResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*plugin.GenerateDependenciesResponse), args.Error(1)
}

func (repo *DependencyResolverMod) CompileAssets(ctx context.Context, inp plugin.CompileAssetsRequest) (*plugin.CompileAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*plugin.CompileAssetsResponse), args.Error(1)
}
