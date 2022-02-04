package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/stretchr/testify/assert"
)

func TestProjectService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   uuid.New(),
		Name: "optimus-project",
	}

	t.Run("Get", func(t *testing.T) {
		t.Run("return error when project name is empty", func(t *testing.T) {
			svc := service.NewProjectService(nil)

			_, err := svc.Get(ctx, "")
			assert.NotNil(t, err)
			assert.Equal(t, "project name cannot be empty", err.Error())
		})
		t.Run("return error when project name is invalid", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, "nonexistent").
				Return(models.ProjectSpec{}, errors.New("invalid project name"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			svc := service.NewProjectService(projectRepoFactory)

			_, err := svc.Get(ctx, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name: project nonexistent not found", err.Error())
		})
		t.Run("return project successfully", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, project.Name).Return(project, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)

			svc := service.NewProjectService(projectRepoFactory)

			actualProject, err := svc.Get(ctx, project.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", actualProject.Name)
			assert.Equal(t, project.ID, actualProject.ID)
		})
	})
}
