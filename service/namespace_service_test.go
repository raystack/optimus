package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

func TestNamespaceService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   uuid.New(),
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
			assert.Equal(t, "project name cannot be empty: invalid argument for entity project", err.Error())
		})
		t.Run("return error when namespace name is empty", func(t *testing.T) {
			svc := service.NewNamespaceService(nil, nil)

			_, err := svc.Get(ctx, "project", "")
			assert.NotNil(t, err)
			assert.Equal(t, "namespace name cannot be empty: invalid argument for entity namespace", err.Error())
		})
		t.Run("return error when repo returns error", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Get", ctx, project.Name, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", models.ProjectSpec{}).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			_, err := svc.Get(ctx, project.Name, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found: not found for entity namespace", err.Error())
		})
		t.Run("return project and namespace successfully", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Get", ctx, project.Name, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", models.ProjectSpec{}).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

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
			assert.Equal(t, "namespace name cannot be empty: invalid argument for entity namespace", err.Error())
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			_, err := svc.GetByName(ctx, project, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found: not found for entity namespace", err.Error())
		})
		t.Run("returns namespace successfully", func(t *testing.T) {
			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

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
			namespaceRepository.On("GetByName", ctx, "nonexistent").
				Return(models.NamespaceSpec{}, store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			_, _, err := svc.GetNamespaceOptionally(ctx, project.Name, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found: not found for entity namespace", err.Error())
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
			namespaceRepository.On("GetByName", ctx, namespace.Name).Return(namespace, nil)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

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
			assert.Equal(t, "namespace name cannot be empty: invalid argument for entity namespace", err.Error())
		})
		t.Run("returns error when fetching project fails", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", ctx, namespace).
				Return(store.ErrResourceNotFound)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			err := svc.Save(ctx, project.Name, namespace)
			assert.NotNil(t, err)
			assert.Equal(t, "resource not found: not found for entity namespace", err.Error())
		})
		t.Run("calls repo to store project successfully", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", ctx, namespace).Return(nil)
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			err := svc.Save(ctx, project.Name, namespace)
			assert.Nil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
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
			namespaceRepo.On("GetAll", ctx).
				Return([]models.NamespaceSpec{}, errors.New("random error"))
			defer namespaceRepo.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepo)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			_, err := svc.GetAll(ctx, project.Name)
			assert.NotNil(t, err)
			assert.Equal(t, "internal error: internal error for entity namespace", err.Error())
		})
		t.Run("return namespaces successfully", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepo := new(mock.NamespaceRepository)
			namespaceRepo.On("GetAll", ctx).
				Return([]models.NamespaceSpec{namespace}, nil)
			defer namespaceRepo.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepo)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			namespaces, err := svc.GetAll(ctx, project.Name)
			assert.Nil(t, err)
			assert.Len(t, namespaces, 1)
			assert.Equal(t, "sample-namespace", namespaces[0].Name)
			assert.Equal(t, namespace.ID, namespaces[0].ID)
		})
	})
}
