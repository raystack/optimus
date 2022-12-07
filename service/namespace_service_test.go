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

func TestNamespaceService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "optimus-project",
	}
	namespace := models.NamespaceSpec{
		ID:   uuid.New(),
		Name: "sample-namespace",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: project,
	}
	projService := new(mock.ProjectService)
	projService.On("Get", ctx, project.Name).Return(project, nil)

	t.Run("Get", func(t *testing.T) {
		t.Run("return error when project name is empty", func(t *testing.T) {
			svc := service.NewNamespaceService(nil, nil)

			_, err := svc.Get(ctx, "", "namespace")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity project: project name cannot be empty", err.Error())
		})
		t.Run("return error when namespace name is empty", func(t *testing.T) {
			svc := service.NewNamespaceService(nil, nil)

			_, err := svc.Get(ctx, "project", "")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity namespace: namespace name cannot be empty", err.Error())
		})
		t.Run("return error when repo returns error", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Get", ctx, project.Name, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			_, err := svc.Get(ctx, project.Name, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "not found for entity namespace: resource not found", err.Error())
		})
		t.Run("return project and namespace successfully", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Get", ctx, project.Name, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			ns, err := svc.Get(ctx, project.Name, namespace.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", ns.ProjectSpec.Name)
			assert.Equal(t, project.ID, ns.ProjectSpec.ID)

			assert.Equal(t, "sample-namespace", ns.Name)
			assert.Equal(t, namespace.ID, ns.ID)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		t.Run("returns error when namespace name is empty", func(t *testing.T) {
			svc := service.NewNamespaceService(nil, nil)

			_, err := svc.GetByName(ctx, project, "")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity namespace: namespace name cannot be empty", err.Error())
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, project, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			_, err := svc.GetByName(ctx, project, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "not found for entity namespace: resource not found", err.Error())
		})
		t.Run("returns namespace successfully", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, project, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			ns, err := svc.GetByName(ctx, project, namespace.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", ns.ProjectSpec.Name)
			assert.Equal(t, project.ID, ns.ProjectSpec.ID)

			assert.Equal(t, "sample-namespace", ns.Name)
			assert.Equal(t, namespace.ID, ns.ID)
		})
	})
	t.Run("GetNamespaceOptionally", func(t *testing.T) {
		t.Run("return error when projectService returns error", func(t *testing.T) {
			projService := new(mock.ProjectService)
			projService.On("Get", ctx, "invalid").
				Return(models.ProjectSpec{}, errors.New("project not found"))
			defer projService.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nil)

			_, _, err := svc.GetNamespaceOptionally(ctx, "invalid", "namespace")
			assert.NotNil(t, err)
			assert.Equal(t, "project not found", err.Error())
		})
		t.Run("return error when repo returns error", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, project, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			_, _, err := svc.GetNamespaceOptionally(ctx, project.Name, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "not found for entity namespace: resource not found", err.Error())
		})
		t.Run("return project when namespace name is empty", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nil)

			proj, ns, err := svc.GetNamespaceOptionally(ctx, project.Name, "")
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", proj.Name)
			assert.Equal(t, project.ID, proj.ID)

			assert.Equal(t, "", ns.Name)
		})
		t.Run("return project and namespace successfully", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, project, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			proj, ns, err := svc.GetNamespaceOptionally(ctx, project.Name, namespace.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", proj.Name)
			assert.Equal(t, project.ID, proj.ID)

			assert.Equal(t, "sample-namespace", ns.Name)
			assert.Equal(t, namespace.ID, ns.ID)
		})
	})
	t.Run("Save", func(t *testing.T) {
		t.Run("return error when namespace name is invalid", func(t *testing.T) {
			ns := models.NamespaceSpec{
				ID:   uuid.New(),
				Name: "",
			}
			svc := service.NewNamespaceService(nil, nil)

			err := svc.Save(ctx, project.Name, ns)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid argument for entity namespace: namespace name cannot be empty", err.Error())
		})
		t.Run("returns error when fetching project fails", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", ctx, project, namespace).
				Return(store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			err := svc.Save(ctx, project.Name, namespace)
			assert.NotNil(t, err)
			assert.Equal(t, "not found for entity namespace: resource not found", err.Error())
		})
		t.Run("calls repo to store project successfully", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", ctx, project, namespace).Return(nil)
			defer namespaceRepository.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepository)

			err := svc.Save(ctx, project.Name, namespace)
			assert.Nil(t, err)
		})
	})
	t.Run("GetAllWithUpstreams", func(t *testing.T) {
		t.Run("return error when getting project fails", func(t *testing.T) {
			projService := new(mock.ProjectService)
			projService.On("Get", ctx, "invalid").
				Return(models.ProjectSpec{}, errors.New("project not found"))
			defer projService.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nil)

			_, err := svc.GetAll(ctx, "invalid")
			assert.NotNil(t, err)
			assert.Equal(t, "project not found", err.Error())
		})
		t.Run("return error when repo has error", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepo := new(mock.NamespaceRepository)
			namespaceRepo.On("GetAllWithUpstreams", ctx, project).
				Return([]models.NamespaceSpec{}, errors.New("random error"))
			defer namespaceRepo.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepo)

			_, err := svc.GetAll(ctx, project.Name)
			assert.NotNil(t, err)
			assert.Equal(t, "internal error for entity namespace: internal error", err.Error())
		})
		t.Run("return namespaces successfully", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepo := new(mock.NamespaceRepository)
			namespaceRepo.On("GetAllWithUpstreams", ctx, project).
				Return([]models.NamespaceSpec{namespace}, nil)
			defer namespaceRepo.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, namespaceRepo)

			namespaces, err := svc.GetAll(ctx, project.Name)
			assert.Nil(t, err)
			assert.Len(t, namespaces, 1)
			assert.Equal(t, "sample-namespace", namespaces[0].Name)
			assert.Equal(t, namespace.ID, namespaces[0].ID)
		})
	})
}
