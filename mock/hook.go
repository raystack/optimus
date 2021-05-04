package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedHookRepo struct {
	mock.Mock
}

func (repo *SupportedHookRepo) GetByName(name string) (models.HookUnit, error) {
	args := repo.Called(name)
	return args.Get(0).(models.HookUnit), args.Error(1)
}

func (repo *SupportedHookRepo) GetAll() []models.HookUnit {
	args := repo.Called()
	return args.Get(0).([]models.HookUnit)
}

func (repo *SupportedHookRepo) Add(t models.HookUnit) error {
	return repo.Called(t).Error(0)
}

type HookUnit struct {
	mock.Mock `hash:"-"`
}

func (repo *HookUnit) Name() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) Image() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) Description() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) AskQuestions(opt models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	args := repo.Called(opt)
	return args.Get(0).(models.AskQuestionResponse), args.Error(1)
}
func (repo *HookUnit) GenerateConfig(inp models.GenerateConfigWithTaskRequest) (models.GenerateConfigResponse, error) {
	args := repo.Called(inp)
	return args.Get(0).(models.GenerateConfigResponse), args.Error(1)
}
func (repo *HookUnit) DependsOn() []string {
	return repo.Called().Get(0).([]string)
}
func (repo *HookUnit) Type() models.HookType {
	return repo.Called().Get(0).(models.HookType)
}
