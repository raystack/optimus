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

func TestNamespaceService(t *testing.T) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   uuid.New(),
		Name: "optimus-project",
	}
	namespace := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "sample-namespace",
		ProjectSpec: project,
	}
	projService := new(mock.ProjectService)
	projService.On("Get", ctx, project.Name).Return(project, nil)

	t.Run("GetProjectAndNamespace", func(t *testing.T) {
		t.Run("return error when namespace name is empty", func(t *testing.T) {
			svc := service.NewNamespaceService(nil, nil)

			_, _, err := svc.GetProjectAndNamespace(ctx, "project", "")
			assert.NotNil(t, err)
			assert.Equal(t, "namespace name cannot be empty", err.Error())
		})
		t.Run("return error when projectService returns error", func(t *testing.T) {
			projService := new(mock.ProjectService)
			projService.On("Get", ctx, "invalid").
				Return(models.ProjectSpec{}, errors.New("project not found"))
			defer projService.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nil)

			_, _, err := svc.GetProjectAndNamespace(ctx, "invalid", "namespace")
			assert.NotNil(t, err)
			assert.Equal(t, "project not found", err.Error())
		})
		t.Run("return error when projectService returns error", func(t *testing.T) {
			defer projService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, "nonexistent").
				Return(models.NamespaceSpec{}, errors.New("invalid namespace name"))
			defer namespaceRepository.AssertExpectations(t)

			nsRepoFactory := new(mock.NamespaceRepoFactory)
			nsRepoFactory.On("New", project).Return(namespaceRepository)
			defer nsRepoFactory.AssertExpectations(t)

			svc := service.NewNamespaceService(projService, nsRepoFactory)

			_, _, err := svc.GetProjectAndNamespace(ctx, project.Name, "nonexistent")
			assert.NotNil(t, err)
			assert.Equal(t, "invalid namespace name", err.Error())
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

			prj, ns, err := svc.GetProjectAndNamespace(ctx, project.Name, namespace.Name)
			assert.Nil(t, err)
			assert.Equal(t, "optimus-project", prj.Name)
			assert.Equal(t, project.ID, prj.ID)

			assert.Equal(t, "sample-namespace", ns.Name)
			assert.Equal(t, namespace.ID, ns.ID)
		})
	})
}
