package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

func TestProjectService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "optimus-project",
	}

	t.Run("Get", func(t *testing.T) {
		t.Run("return error when project name is empty", func(t *testing.T) {
			svc := service.NewProjectService(nil)

			_, err := svc.Get(ctx, "")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity project: project name cannot be empty", err.Error())
		})
		t.Run("return error when project name is invalid", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, "nonexistent").
				Return(models.ProjectSpec{}, store.ErrResourceNotFound)
			defer projectRepository.AssertExpectations(t)

			svc := service.NewProjectService(projectRepository)

			_, err := svc.Get(ctx, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "not found for entity project: resource not found", err.Error())
		})
		t.Run("return project successfully", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, project.Name).Return(project, nil)
			defer projectRepository.AssertExpectations(t)

			svc := service.NewProjectService(projectRepository)

			actualProject, err := svc.Get(ctx, project.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", actualProject.Name)
			assert.Equal(t, project.ID, actualProject.ID)
		})
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("return error when project name is invalid", func(t *testing.T) {
			proj := models.ProjectSpec{
				Name: "",
				ID:   models.ProjectID(uuid.New()),
			}
			svc := service.NewProjectService(nil)

			err := svc.Save(ctx, proj)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity project: project name cannot be empty", err.Error())
		})
		t.Run("calls repo to store project successfully", func(t *testing.T) {
			project2 := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "optimus-project",
				Config: map[string]string{
					"bucket":   "gs://some_folder",
					"kafkaKey": "10.12.12.12:6668,10.12.12.13:6668",
				},
			}
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", ctx, project2).Return(nil)
			defer projectRepository.AssertExpectations(t)

			svc := service.NewProjectService(projectRepository)

			err := svc.Save(ctx, project2)
			assert.Nil(t, err)
		})
	})
	t.Run("GetAllWithUpstreams", func(t *testing.T) {
		t.Run("return error when repo has error", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).
				Return([]models.ProjectSpec{}, errors.New("random error"))
			defer projectRepository.AssertExpectations(t)

			svc := service.NewProjectService(projectRepository)

			_, err := svc.GetAll(ctx)
			assert.NotNil(t, err)
			assert.Equal(t, "internal error for entity project: internal error", err.Error())
		})
		t.Run("return projects successfully", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return([]models.ProjectSpec{project}, nil)
			defer projectRepository.AssertExpectations(t)

			svc := service.NewProjectService(projectRepository)

			prjs, err := svc.GetAll(ctx)
			assert.Nil(t, err)
			assert.Len(t, prjs, 1)
			assert.Equal(t, "optimus-project", prjs[0].Name)
			assert.Equal(t, project.ID, prjs[0].ID)
		})
	})
}
