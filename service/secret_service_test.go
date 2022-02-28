package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/stretchr/testify/assert"
)

func TestSecretService(t *testing.T) {
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

	secretItems := []models.SecretItemInfo{
		{
			ID:        uuid.New(),
			Name:      "secret1",
			Digest:    "IKorImxRlxzzn0xT0bbVnYshirrTHLtIpWPiK/+e/+8=",
			Type:      models.SecretTypeUserDefined,
			Namespace: namespace.Name,
			UpdatedAt: time.Now(),
		},
	}

	emptySecret := models.ProjectSecretItem{Name: "", Value: ""}
	secret1 := models.ProjectSecretItem{Name: "secret1", Value: "secret1"}

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			svc := service.NewSecretService(nil, nil, nil)

			err := svc.Save(ctx, "local", "first", emptySecret)
			assert.NotNil(t, err)
			assert.Equal(t, "secret name cannot be empty: invalid argument for entity secret", err.Error())
		})
		t.Run("returns error when namespace service has error", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, "local", "first").
				Return(models.NamespaceSpec{}, errors.New("invalid project name"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Save(ctx, "local", "first", secret1)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name", err.Error())
		})
		t.Run("saves the secret when no namespace", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, project.Name).
				Return(project, nil)
			defer projectService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Save", ctx, models.NamespaceSpec{}, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(projectService, nil, secretRepoFac)

			err := svc.Save(ctx, project.Name, "", secret1)
			assert.Nil(t, err)
		})
		t.Run("saves the secret item successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, project.Name, namespace.Name).
				Return(namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Save", ctx, namespace, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepoFac)

			err := svc.Save(ctx, project.Name, namespace.Name, secret1)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			svc := service.NewSecretService(nil, nil, nil)

			err := svc.Update(ctx, "local", "first", emptySecret)
			assert.NotNil(t, err)
			assert.Equal(t, "secret name cannot be empty: invalid argument for entity secret", err.Error())
		})
		t.Run("returns error when namespace service has error", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, "local", "first").
				Return(models.NamespaceSpec{}, errors.New("invalid project name"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Update(ctx, "local", "first", secret1)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name", err.Error())
		})
		t.Run("updates the secret item when no namespace", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, project.Name).
				Return(project, nil)
			defer projectService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Update", ctx, models.NamespaceSpec{}, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(projectService, nil, secretRepoFac)

			err := svc.Update(ctx, project.Name, "", secret1)
			assert.Nil(t, err)
		})
		t.Run("updates the secret item successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, project.Name, namespace.Name).
				Return(namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Update", ctx, namespace, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepoFac)

			err := svc.Update(ctx, project.Name, namespace.Name, secret1)
			assert.Nil(t, err)
		})
	})
	t.Run("List", func(t *testing.T) {
		t.Run("returns error when project service has error", func(t *testing.T) {
			projService := new(mock.ProjectService)
			projService.On("Get", ctx, project.Name).
				Return(models.ProjectSpec{}, errors.New("error in getting project"))
			defer projService.AssertExpectations(t)

			svc := service.NewSecretService(projService, nil, nil)

			_, err := svc.List(ctx, project.Name)
			assert.NotNil(t, err)
			assert.Equal(t, "error in getting project", err.Error())
		})
		t.Run("returns list of secrets", func(t *testing.T) {
			projService := new(mock.ProjectService)
			projService.On("Get", ctx, project.Name).Return(project, nil)
			defer projService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("GetAll", ctx).Return(secretItems, nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(projService, nil, secretRepoFac)

			list, err := svc.List(ctx, project.Name)
			assert.Nil(t, err)

			assert.Len(t, list, 1)
			assert.Equal(t, secretItems, list)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("returns error when error during getting namespace", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetOptional", ctx, project.Name, "").
				Return(models.NamespaceSpec{}, errors.New("error in getting project"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Delete(ctx, project.Name, "", "hello")
			assert.NotNil(t, err)
			assert.Equal(t, "error in getting project", err.Error())
		})
		t.Run("deletes the secret successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetOptional", ctx, project.Name, "namespace").Return(namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Delete", ctx, namespace, "hello").Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepoFac)

			err := svc.Delete(ctx, project.Name, "namespace", "hello")
			assert.Nil(t, err)
		})
	})
}
