package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
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
			nsService.On("GetNamespaceOptionally", ctx, "local", "first").
				Return(models.ProjectSpec{}, models.NamespaceSpec{}, errors.New("invalid project name"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Save(ctx, "local", "first", secret1)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name", err.Error())
		})
		t.Run("saves the secret when no namespace", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, "").
				Return(project, models.NamespaceSpec{}, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Save", ctx, project, models.NamespaceSpec{}, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepo)

			err := svc.Save(ctx, project.Name, "", secret1)
			assert.Nil(t, err)
		})
		t.Run("saves the secret item successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, namespace.Name).
				Return(project, namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Save", ctx, project, namespace, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepo)

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
			nsService.On("GetNamespaceOptionally", ctx, "local", "first").
				Return(models.ProjectSpec{}, models.NamespaceSpec{}, errors.New("invalid project name"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Update(ctx, "local", "first", secret1)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name", err.Error())
		})
		t.Run("updates the secret item when no namespace", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, "").
				Return(project, models.NamespaceSpec{}, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Update", ctx, project, models.NamespaceSpec{}, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepo)

			err := svc.Update(ctx, project.Name, "", secret1)
			assert.Nil(t, err)
		})
		t.Run("updates the secret item successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, namespace.Name).
				Return(project, namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Update", ctx, project, namespace, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepo)

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
			secretRepo.On("GetAll", ctx, project).Return(secretItems, nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(projService, nil, secretRepo)

			list, err := svc.List(ctx, project.Name)
			assert.Nil(t, err)

			assert.Len(t, list, 1)
			assert.Equal(t, secretItems, list)
		})
	})
	t.Run("GetSecrets", func(t *testing.T) {
		t.Run("returns secrets for a namespace", func(t *testing.T) {
			secrets := []models.ProjectSecretItem{
				{
					ID:    uuid.New(),
					Name:  "secret1",
					Value: "secret1",
					Type:  models.SecretTypeUserDefined,
				},
			}
			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("GetSecrets", ctx, project, namespace).Return(secrets, nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nil, secretRepo)

			list, err := svc.GetSecrets(ctx, namespace)
			assert.Nil(t, err)

			assert.Len(t, list, 1)
			assert.Equal(t, secrets, list)
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("GetSecrets", ctx, project, namespace).Return([]models.ProjectSecretItem{},
				errors.New("random error"))
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nil, secretRepo)

			list, err := svc.GetSecrets(ctx, namespace)
			assert.Len(t, list, 0)

			assert.NotNil(t, err)
			assert.Equal(t, "error while getting secrets: internal error for entity secret", err.Error())
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("returns error when error during getting namespace", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, "").
				Return(models.ProjectSpec{}, models.NamespaceSpec{}, errors.New("error in getting project"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, nil)

			err := svc.Delete(ctx, project.Name, "", "hello")
			assert.NotNil(t, err)
			assert.Equal(t, "error in getting project", err.Error())
		})
		t.Run("deletes the secret successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetNamespaceOptionally", ctx, project.Name, "namespace").Return(project, namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Delete", ctx, project, namespace, "hello").Return(nil)
			defer secretRepo.AssertExpectations(t)

			svc := service.NewSecretService(nil, nsService, secretRepo)

			err := svc.Delete(ctx, project.Name, "namespace", "hello")
			assert.Nil(t, err)
		})
	})
}
