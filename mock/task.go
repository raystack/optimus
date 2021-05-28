package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedTaskRepo struct {
	mock.Mock
}

func (repo *SupportedTaskRepo) GetByName(name string) (models.TaskPlugin, error) {
	args := repo.Called(name)
	return args.Get(0).(models.TaskPlugin), args.Error(1)
}

func (repo *SupportedTaskRepo) GetAll() []models.TaskPlugin {
	args := repo.Called()
	return args.Get(0).([]models.TaskPlugin)
}

func (repo *SupportedTaskRepo) Add(t models.TaskPlugin) error {
	return repo.Called(t).Error(0)
}

type TaskPlugin struct {
	mock.Mock `hash:"-"`
}

func (repo *TaskPlugin) GetTaskSchema(ctx context.Context, inp models.GetTaskSchemaRequest) (models.GetTaskSchemaResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.GetTaskSchemaResponse), args.Error(1)
}

func (repo *TaskPlugin) DefaultTaskConfig(ctx context.Context, inp models.DefaultTaskConfigRequest) (models.DefaultTaskConfigResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.DefaultTaskConfigResponse), args.Error(1)
}

func (repo *TaskPlugin) DefaultTaskAssets(ctx context.Context, inp models.DefaultTaskAssetsRequest) (models.DefaultTaskAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.DefaultTaskAssetsResponse), args.Error(1)
}

func (repo *TaskPlugin) CompileTaskAssets(ctx context.Context, inp models.CompileTaskAssetsRequest) (models.CompileTaskAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.CompileTaskAssetsResponse), args.Error(1)
}

func (repo *TaskPlugin) GetTaskQuestions(ctx context.Context, inp models.GetTaskQuestionsRequest) (models.GetTaskQuestionsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.GetTaskQuestionsResponse), args.Error(1)
}

func (repo *TaskPlugin) ValidateTaskQuestion(ctx context.Context, inp models.ValidateTaskQuestionRequest) (models.ValidateTaskQuestionResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.ValidateTaskQuestionResponse), args.Error(1)
}

func (repo *TaskPlugin) GenerateTaskDestination(ctx context.Context, inp models.GenerateTaskDestinationRequest) (models.GenerateTaskDestinationResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.GenerateTaskDestinationResponse), args.Error(1)
}

func (repo *TaskPlugin) GenerateTaskDependencies(ctx context.Context, inp models.GenerateTaskDependenciesRequest) (models.GenerateTaskDependenciesResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(models.GenerateTaskDependenciesResponse), args.Error(1)
}
