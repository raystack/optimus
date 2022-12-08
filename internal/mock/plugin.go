package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/internal/models"
)

type YamlMod struct {
	mock.Mock `hash:"-"`
}

func (repo *YamlMod) PluginInfo() *models.PluginInfoResponse {
	args := repo.Called()
	return args.Get(0).(*models.PluginInfoResponse)
}

func (repo *YamlMod) DefaultConfig(ctx context.Context, inp models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.DefaultConfigResponse), args.Error(1)
}

func (repo *YamlMod) DefaultAssets(ctx context.Context, inp models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.DefaultAssetsResponse), args.Error(1)
}

func (repo *YamlMod) GetQuestions(ctx context.Context, inp models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.GetQuestionsResponse), args.Error(1)
}

func (repo *YamlMod) ValidateQuestion(ctx context.Context, inp models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.ValidateQuestionResponse), args.Error(1)
}

type DependencyResolverMod struct {
	mock.Mock `hash:"-"`
}

func (repo *DependencyResolverMod) GetName(ctx context.Context) (string, error) {
	args := repo.Called(ctx)
	return args.Get(0).(string), args.Error(1)
}

func (repo *DependencyResolverMod) GenerateDestination(ctx context.Context, inp models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.GenerateDestinationResponse), args.Error(1)
}

func (repo *DependencyResolverMod) GenerateDependencies(ctx context.Context, inp models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.GenerateDependenciesResponse), args.Error(1)
}

func (repo *DependencyResolverMod) CompileAssets(ctx context.Context, inp models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.CompileAssetsResponse), args.Error(1)
}
