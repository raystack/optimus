package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/core/tenant/handler/v1beta1"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestProjectHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	conf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	savedProject, _ := tenant.NewProject("savedProj", conf)

	t.Run("RegisterProject", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			projectService := new(projectService)
			handler := v1beta1.NewProjectHandler(logger, projectService)

			registerReq := pb.RegisterProjectRequest{Project: &pb.ProjectSpecification{
				Name:   "",
				Config: conf,
			}}

			_, err := handler.RegisterProject(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: not able to register project ")
		})
		t.Run("returns error when fails in service", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("Save", ctx, mock.Anything).Return(errors.New("error in saving"))
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			registerReq := pb.RegisterProjectRequest{Project: &pb.ProjectSpecification{
				Name:   "proj",
				Config: conf,
			}}

			_, err := handler.RegisterProject(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in saving: not able "+
				"to register project proj")
		})
		t.Run("saves the project successfully", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("Save", ctx, mock.Anything).Return(nil)
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			registerReq := pb.RegisterProjectRequest{Project: &pb.ProjectSpecification{
				Name:   "proj",
				Config: conf,
			}}

			_, err := handler.RegisterProject(ctx, &registerReq)
			assert.Nil(t, err)
		})
	})
	t.Run("ListProjects", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("GetAll", ctx).Return(nil, errors.New("unable to fetch"))
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			_, err := handler.ListProjects(ctx, &pb.ListProjectsRequest{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = unable to fetch: failed to "+
				"retrieve saved projects")
		})
		t.Run("returns the list of saved projects", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("GetAll", ctx).
				Return([]*tenant.Project{savedProject}, nil)
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			projRes, err := handler.ListProjects(ctx, &pb.ListProjectsRequest{})
			assert.Nil(t, err)

			assert.Equal(t, len(projRes.Projects), 1)
			assert.Equal(t, projRes.Projects[0].Name, savedProject.Name().String())
		})
	})
	t.Run("GetProject", func(t *testing.T) {
		t.Run("returns error when project name is empty", func(t *testing.T) {
			projectService := new(projectService)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			_, err := handler.GetProject(ctx, &pb.GetProjectRequest{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to retrieve project []")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("Get", ctx, tenant.ProjectName("savedProj")).
				Return(nil, errors.New("random error"))
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			_, err := handler.GetProject(ctx, &pb.GetProjectRequest{ProjectName: "savedProj"})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = random error: failed to "+
				"retrieve project [savedProj]")
		})
		t.Run("returns the project successfully", func(t *testing.T) {
			projectService := new(projectService)
			projectService.On("Get", ctx, tenant.ProjectName("savedProj")).Return(savedProject, nil)
			defer projectService.AssertExpectations(t)

			handler := v1beta1.NewProjectHandler(logger, projectService)

			proj, err := handler.GetProject(ctx, &pb.GetProjectRequest{
				ProjectName: "savedProj",
			})
			assert.Nil(t, err)

			assert.Equal(t, savedProject.Name().String(), proj.Project.GetName())
		})
	})
}

type projectService struct {
	mock.Mock
}

func (p *projectService) Save(ctx context.Context, project *tenant.Project) error {
	args := p.Called(ctx, project)
	return args.Error(0)
}

func (p *projectService) Get(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	args := p.Called(ctx, name)
	var prj *tenant.Project
	if args.Get(0) != nil {
		prj = args.Get(0).(*tenant.Project)
	}
	return prj, args.Error(1)
}

func (p *projectService) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	args := p.Called(ctx)
	var prjs []*tenant.Project
	if args.Get(0) != nil {
		prjs = args.Get(0).([]*tenant.Project)
	}
	return prjs, args.Error(1)
}
