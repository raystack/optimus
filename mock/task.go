package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type SupportedTransformationRepo struct {
	mock.Mock
}

func (repo *SupportedTransformationRepo) GetByName(name string) (models.Transformation, error) {
	args := repo.Called(name)
	return args.Get(0).(models.Transformation), args.Error(1)
}

func (repo *SupportedTransformationRepo) GetAll() []models.Transformation {
	args := repo.Called()
	return args.Get(0).([]models.Transformation)
}

func (repo *SupportedTransformationRepo) Add(t models.Transformation) error {
	return repo.Called(t).Error(0)
}

type Transformer struct {
	mock.Mock
}

func (repo *Transformer) Name() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) Image() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) GenerateAssets(inp models.GenerateAssetsRequest) (models.GenerateAssetsResponse, error) {
	args := repo.Called(inp)
	return args.Get(0).(models.GenerateAssetsResponse), args.Error(1)
}
func (repo *Transformer) Description() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) AskQuestions(opt models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	args := repo.Called(opt)
	return args.Get(0).(models.AskQuestionResponse), args.Error(1)
}
func (repo *Transformer) GenerateConfig(inp models.GenerateConfigRequest) (models.GenerateConfigResponse, error) {
	args := repo.Called(inp)
	return args.Get(0).(models.GenerateConfigResponse), args.Error(1)
}
func (repo *Transformer) GenerateDestination(data models.GenerateDestinationRequest) (models.GenerateDestinationResponse, error) {
	args := repo.Called(data)
	return args.Get(0).(models.GenerateDestinationResponse), args.Error(1)
}
func (repo *Transformer) GenerateDependencies(data models.GenerateDependenciesRequest) (models.GenerateDependenciesResponse, error) {
	args := repo.Called(data)
	return args.Get(0).(models.GenerateDependenciesResponse), args.Error(1)
}
