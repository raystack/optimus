package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedTaskRepo struct {
	mock.Mock
}

func (repo *SupportedTaskRepo) GetByName(name string) (models.Transformation, error) {
	args := repo.Called(name)
	return args.Get(0).(models.Transformation), args.Error(1)
}

func (repo *SupportedTaskRepo) GetAll() []models.Transformation {
	args := repo.Called()
	return args.Get(0).([]models.Transformation)
}

func (repo *SupportedTaskRepo) Add(t models.Transformation) error {
	return repo.Called(t).Error(0)
}

type ExecutionUnit struct {
	mock.Mock
}

func (repo *ExecutionUnit) GetName() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *ExecutionUnit) GetImage() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *ExecutionUnit) GenerateAssets(inp map[string]interface{}, opt models.UnitOptions) (map[string]string, error) {
	args := repo.Called(inp, opt)
	return args.Get(0).(map[string]string), args.Error(1)
}
func (repo *ExecutionUnit) GetDescription() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *ExecutionUnit) AskQuestions(opt models.UnitOptions) (map[string]interface{}, error) {
	args := repo.Called(opt)
	return args.Get(0).(map[string]interface{}), args.Error(1)
}
func (repo *ExecutionUnit) GenerateConfig(inp map[string]interface{}, opt models.UnitOptions) (models.JobSpecConfigs, error) {
	args := repo.Called(inp, opt)
	return args.Get(0).(models.JobSpecConfigs), args.Error(1)
}
func (repo *ExecutionUnit) GenerateDestination(data models.UnitData) (string, error) {
	args := repo.Called(data)
	return args.Get(0).(string), args.Error(1)
}
func (repo *ExecutionUnit) GenerateDependencies(data models.UnitData) ([]string, error) {
	args := repo.Called(data)
	return args.Get(0).([]string), args.Error(1)
}
