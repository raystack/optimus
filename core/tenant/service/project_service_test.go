package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/core/tenant/service"
)

func TestProjectService(t *testing.T) {
	ctx := context.Background()
	conf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	savedProject, _ := tenant.NewProject("savedProj", conf)

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when fails in service", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("Save", ctx, mock.Anything).Return(errors.New("error in saving"))
			defer projectRepo.AssertExpectations(t)

			toSaveProj, _ := tenant.NewProject("proj", map[string]string{"BUCKET": "gs://some_folder"})

			projService := service.NewProjectService(projectRepo)
			err := projService.Save(ctx, toSaveProj)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in saving")
		})
		t.Run("saves the project successfully", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("Save", ctx, mock.Anything).Return(nil)
			defer projectRepo.AssertExpectations(t)

			toSaveProj, _ := tenant.NewProject("proj", map[string]string{"BUCKET": "gs://some_folder"})

			projService := service.NewProjectService(projectRepo)
			err := projService.Save(ctx, toSaveProj)

			assert.Nil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetAll", ctx).
				Return(nil, errors.New("error in getting all"))
			defer projectRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo)
			_, err := projService.GetAll(ctx)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting all")
		})
		t.Run("returns the list of saved projects", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetAll", ctx).
				Return([]*tenant.Project{savedProject}, nil)
			defer projectRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo)
			projs, err := projService.GetAll(ctx)

			assert.Nil(t, err)
			assert.Equal(t, 1, len(projs))
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetByName", ctx, tenant.ProjectName("savedProj")).
				Return(nil, errors.New("error in getting"))
			defer projectRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo)
			_, err := projService.Get(ctx, savedProject.Name())

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting")
		})
		t.Run("returns the project successfully", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetByName", ctx, tenant.ProjectName("savedProj")).Return(savedProject, nil)
			defer projectRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo)
			proj, err := projService.Get(ctx, savedProject.Name())

			assert.Nil(t, err)
			assert.Equal(t, savedProject.Name(), proj.Name())
		})
	})
}

type projectRepo struct {
	mock.Mock
}

func (p *projectRepo) Save(ctx context.Context, project *tenant.Project) error {
	args := p.Called(ctx, project)
	return args.Error(0)
}

func (p *projectRepo) GetByName(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	args := p.Called(ctx, name)
	var prj *tenant.Project
	if args.Get(0) != nil {
		prj = args.Get(0).(*tenant.Project)
	}
	return prj, args.Error(1)
}

func (p *projectRepo) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	args := p.Called(ctx)
	var prjs []*tenant.Project
	if args.Get(0) != nil {
		prjs = args.Get(0).([]*tenant.Project)
	}
	return prjs, args.Error(1)
}
