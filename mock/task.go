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
	mock.Mock `hash:"-"`
}

func (repo *Transformer) Name() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) Image() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) Description() string {
	args := repo.Called()
	return args.Get(0).(string)
}
func (repo *Transformer) AskQuestions(opt models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	args := repo.Called(opt)
	return args.Get(0).(models.AskQuestionResponse), args.Error(1)
}
func (repo *Transformer) DefaultConfig(inp models.DefaultConfigRequest) (models.DefaultConfigResponse, error) {
	args := repo.Called(inp)
	return args.Get(0).(models.DefaultConfigResponse), args.Error(1)
}
func (repo *Transformer) DefaultAssets(inp models.DefaultAssetsRequest) (models.DefaultAssetsResponse, error) {
	args := repo.Called(inp)
	return args.Get(0).(models.DefaultAssetsResponse), args.Error(1)
}
func (repo *Transformer) CompileAssets(request models.CompileAssetsRequest) (models.CompileAssetsResponse, error) {
	args := repo.Called(request)
	return args.Get(0).(models.CompileAssetsResponse), args.Error(1)
}
func (repo *Transformer) GenerateDestination(data models.GenerateDestinationRequest) (models.GenerateDestinationResponse, error) {
	args := repo.Called(data)
	return args.Get(0).(models.GenerateDestinationResponse), args.Error(1)
}
func (repo *Transformer) GenerateDependencies(data models.GenerateDependenciesRequest) (models.GenerateDependenciesResponse, error) {
	args := repo.Called(data)
	return args.Get(0).(models.GenerateDependenciesResponse), args.Error(1)
}
