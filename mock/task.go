package mock

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedTaskRepo struct {
	mock.Mock
}

func (repo *SupportedTaskRepo) GetByName(name string) (models.ExecUnit, error) {
	args := repo.Called(name)
	return args.Get(0).(models.ExecUnit), args.Error(1)
}

func (repo *SupportedTaskRepo) GetAll() []models.ExecUnit {
	args := repo.Called()
	return args.Get(0).([]models.ExecUnit)
}

func (repo *SupportedTaskRepo) Add(t models.ExecUnit) error {
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
func (repo *ExecutionUnit) GetAssets() map[string]string {
	args := repo.Called()
	return args.Get(0).(map[string]string)
}
func (repo *ExecutionUnit) GetDescription() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *ExecutionUnit) GetQuestions() []*survey.Question {
	args := repo.Called()
	return args.Get(0).([]*survey.Question)
}
func (repo *ExecutionUnit) GetConfig() map[string]string {
	args := repo.Called()
	return args.Get(0).(map[string]string)
}
func (repo *ExecutionUnit) GenerateDestination(data models.UnitData) (string, error) {
	args := repo.Called(data)
	return args.Get(0).(string), args.Error(1)
}
func (repo *ExecutionUnit) GenerateDependencies(data models.UnitData) ([]string, error) {
	args := repo.Called(data)
	return args.Get(0).([]string), args.Error(1)
}
