package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

// PluginRepository is an autogenerated mock type for the PluginRepository type
type PluginRepository struct {
	mock.Mock
}

// Add provides a mock function with given fields: _a0, _a1
func (_m *PluginRepository) Add(_a0 models.BasePlugin, _a1 models.DependencyResolverMod) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(models.BasePlugin, models.DependencyResolverMod) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddYaml provides a mock function with given fields: _a0
func (_m *PluginRepository) AddYaml(_a0 models.YamlMod) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(models.YamlMod) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAll provides a mock function with given fields:
func (_m *PluginRepository) GetAll() []*models.Plugin {
	ret := _m.Called()

	var r0 []*models.Plugin
	if rf, ok := ret.Get(0).(func() []*models.Plugin); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]*models.Plugin)
	}

	return r0
}

// GetByName provides a mock function with given fields: _a0
func (_m *PluginRepository) GetByName(_a0 string) (*models.Plugin, error) {
	ret := _m.Called(_a0)

	var r0 *models.Plugin
	if rf, ok := ret.Get(0).(func(string) *models.Plugin); ok {
		r0 = rf(_a0)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(*models.Plugin)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetHooks provides a mock function with given fields:
func (_m *PluginRepository) GetHooks() []*models.Plugin {
	ret := _m.Called()

	var r0 []*models.Plugin
	if rf, ok := ret.Get(0).(func() []*models.Plugin); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]*models.Plugin)
	}

	return r0
}

// GetTasks provides a mock function with given fields:
func (_m *PluginRepository) GetTasks() []*models.Plugin {
	ret := _m.Called()

	var r0 []*models.Plugin
	if rf, ok := ret.Get(0).(func() []*models.Plugin); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]*models.Plugin)
	}

	return r0
}

type mockConstructorTestingTNewPluginRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewPluginRepository creates a new instance of PluginRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPluginRepository(t mockConstructorTestingTNewPluginRepository) *PluginRepository {
	mock := &PluginRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type BasePlugin struct {
	mock.Mock `hash:"-"`
}

func (repo *BasePlugin) PluginInfo() (*models.PluginInfoResponse, error) {
	args := repo.Called()
	return args.Get(0).(*models.PluginInfoResponse), args.Error(1)
}

type CLIMod struct {
	mock.Mock `hash:"-"`
}

func (repo *CLIMod) PluginInfo() (*models.PluginInfoResponse, error) {
	args := repo.Called()
	return args.Get(0).(*models.PluginInfoResponse), args.Error(1)
}

func (repo *CLIMod) DefaultConfig(ctx context.Context, inp models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.DefaultConfigResponse), args.Error(1)
}

func (repo *CLIMod) DefaultAssets(ctx context.Context, inp models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.DefaultAssetsResponse), args.Error(1)
}

func (repo *CLIMod) GetQuestions(ctx context.Context, inp models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.GetQuestionsResponse), args.Error(1)
}

func (repo *CLIMod) ValidateQuestion(ctx context.Context, inp models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	args := repo.Called(ctx, inp)
	return args.Get(0).(*models.ValidateQuestionResponse), args.Error(1)
}

type DependencyResolverMod struct {
	mock.Mock `hash:"-"`
}

func (repo *DependencyResolverMod) PluginInfo() (*models.PluginInfoResponse, error) {
	args := repo.Called()
	return args.Get(0).(*models.PluginInfoResponse), args.Error(1)
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

// PluginService is an autogenerated mock type for the PluginService type
type PluginService struct {
	mock.Mock
}

// GenerateDependencies provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *PluginService) GenerateDependencies(_a0 context.Context, _a1 models.JobSpec, _a2 models.NamespaceSpec, _a3 bool) (*models.GenerateDependenciesResponse, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	var r0 *models.GenerateDependenciesResponse
	if rf, ok := ret.Get(0).(func(context.Context, models.JobSpec, models.NamespaceSpec, bool) *models.GenerateDependenciesResponse); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(*models.GenerateDependenciesResponse)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.JobSpec, models.NamespaceSpec, bool) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GenerateDestination provides a mock function with given fields: _a0, _a1, _a2
func (_m *PluginService) GenerateDestination(_a0 context.Context, _a1 models.JobSpec, _a2 models.NamespaceSpec) (*models.GenerateDestinationResponse, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *models.GenerateDestinationResponse
	if rf, ok := ret.Get(0).(func(context.Context, models.JobSpec, models.NamespaceSpec) *models.GenerateDestinationResponse); ok {
		r0 = rf(_a0, _a1, _a2)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(*models.GenerateDestinationResponse)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.JobSpec, models.NamespaceSpec) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewPluginService interface {
	mock.TestingT
	Cleanup(func())
}

// NewPluginService creates a new instance of PluginService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPluginService(t mockConstructorTestingTNewPluginService) *PluginService {
	mock := &PluginService{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
