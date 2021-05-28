package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedHookRepo struct {
	mock.Mock
}

func (repo *SupportedHookRepo) GetByName(name string) (models.HookPlugin, error) {
	args := repo.Called(name)
	return args.Get(0).(models.HookPlugin), args.Error(1)
}

func (repo *SupportedHookRepo) GetAll() []models.HookPlugin {
	args := repo.Called()
	return args.Get(0).([]models.HookPlugin)
}

func (repo *SupportedHookRepo) Add(t models.HookPlugin) error {
	return repo.Called(t).Error(0)
}

type HookPlugin struct {
	mock.Mock `hash:"-"`
}

func (repo *HookPlugin) GetHookSchema(ctx context.Context, request models.GetHookSchemaRequest) (models.GetHookSchemaResponse, error) {
	args := repo.Called(ctx, request)
	return args.Get(0).(models.GetHookSchemaResponse), args.Error(1)
}

func (repo *HookPlugin) GetHookQuestions(ctx context.Context, request models.GetHookQuestionsRequest) (models.GetHookQuestionsResponse, error) {
	args := repo.Called(ctx, request)
	return args.Get(0).(models.GetHookQuestionsResponse), args.Error(1)
}

func (repo *HookPlugin) ValidateHookQuestion(ctx context.Context, request models.ValidateHookQuestionRequest) (models.ValidateHookQuestionResponse, error) {
	args := repo.Called(ctx, request)
	return args.Get(0).(models.ValidateHookQuestionResponse), args.Error(1)
}

func (repo *HookPlugin) DefaultHookConfig(ctx context.Context, request models.DefaultHookConfigRequest) (models.DefaultHookConfigResponse, error) {
	args := repo.Called(ctx, request)
	return args.Get(0).(models.DefaultHookConfigResponse), args.Error(1)
}

func (repo *HookPlugin) DefaultHookAssets(ctx context.Context, request models.DefaultHookAssetsRequest) (models.DefaultHookAssetsResponse, error) {
	args := repo.Called(ctx, request)
	return args.Get(0).(models.DefaultHookAssetsResponse), args.Error(1)
}
