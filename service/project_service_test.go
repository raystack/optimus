package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/assert"
)

func TestProjectService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   uuid.New(),
		Name: "optimus-project",
	}

	t.Run("GetByName", func(t *testing.T) {
		t.Run("return error when project name is empty", func(t *testing.T) {
			svc := service.NewProjectService(nil)

			_, err := svc.GetByName(ctx, "")
			assert.NotNil(t, err)
			assert.Equal(t, "project name cannot be empty: invalid argument for entity project", err.Error())
		})
		t.Run("return error when project name is invalid", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, "nonexistent").
				Return(models.ProjectSpec{}, store.ErrResourceNotFound)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			svc := service.NewProjectService(projectRepoFactory)

			_, err := svc.GetByName(ctx, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found: not found for entity project", err.Error())
		})
		t.Run("return project successfully", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, project.Name).Return(project, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)

			svc := service.NewProjectService(projectRepoFactory)

			actualProject, err := svc.GetByName(ctx, project.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", actualProject.Name)
			assert.Equal(t, project.ID, actualProject.ID)
		})
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("return error when project name is invalid", func(t *testing.T) {
			proj := models.ProjectSpec{
				Name: "",
				ID:   uuid.New(),
			}
			svc := service.NewProjectService(nil)

			err := svc.Save(ctx, proj)
			assert.NotNil(t, err)
			assert.Equal(t, "project name cannot be empty: invalid argument for entity project", err.Error())
		})
		t.Run("calls repo to store project successfully", func(t *testing.T) {
			project2 := models.ProjectSpec{
				ID:   uuid.New(),
				Name: "optimus-project",
				Config: map[string]string{
					"bucket":   "gs://some_folder",
					"kafkaKey": "10.12.12.12:6668,10.12.12.13:6668",
				},
			}
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", ctx, project2).Return(nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)

			svc := service.NewProjectService(projectRepoFactory)

			err := svc.Save(ctx, project2)
			assert.Nil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("return error when repo has error", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAll", ctx).
				Return([]models.ProjectSpec{}, errors.New("random error"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			svc := service.NewProjectService(projectRepoFactory)

			_, err := svc.GetAll(ctx)
			assert.NotNil(t, err)
			assert.Equal(t, "internal error: internal error for entity project", err.Error())
		})
		t.Run("return projects successfully", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAll", ctx).Return([]models.ProjectSpec{project}, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)

			svc := service.NewProjectService(projectRepoFactory)

			prjs, err := svc.GetAll(ctx)
			assert.Nil(t, err)
			assert.Len(t, prjs, 1)
			assert.Equal(t, "optimus-project", prjs[0].Name)
			assert.Equal(t, project.ID, prjs[0].ID)
		})
	})
}
