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
	mock.Mock
}

func (repo *HookUnit) GetName() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) GetImage() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) GetDescription() string {
	return repo.Called().Get(0).(string)
}
func (repo *HookUnit) AskQuestions(opt models.UnitOptions) (map[string]interface{}, error) {
	args := repo.Called(opt)
	return args.Get(0).(map[string]interface{}), args.Error(1)
}
func (repo *HookUnit) GenerateConfig(inp map[string]interface{}, jobUnitData models.UnitData) (models.JobSpecConfigs, error) {
	args := repo.Called(inp, jobUnitData)
	return args.Get(0).(models.JobSpecConfigs), args.Error(1)
}
func (repo *HookUnit) GetDependsOn() []string {
	return repo.Called().Get(0).([]string)
}
func (repo *HookUnit) GetType() models.HookType {
	return repo.Called().Get(0).(models.HookType)
}
