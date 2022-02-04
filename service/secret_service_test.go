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

	emptySecret := models.ProjectSecretItem{Name: "", Value: ""}
	secret1 := models.ProjectSecretItem{Name: "secret1", Value: "secret1"}

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			svc := service.NewSecretService(nil, nil)

			err := svc.Save(ctx, "local", "first", emptySecret)
			assert.NotNil(t, err)
			assert.Equal(t, "secret name cannot be empty", err.Error())
		})
		t.Run("returns error when namespace service has error", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetProjectAndNamespace", ctx, "local", "first").
				Return(models.ProjectSpec{}, models.NamespaceSpec{}, errors.New("invalid project name"))
			defer nsService.AssertExpectations(t)

			svc := service.NewSecretService(nsService, nil)

			err := svc.Save(ctx, "local", "first", secret1)
			assert.NotNil(t, err)
			assert.Equal(t, "invalid project name", err.Error())
		})

		t.Run("saves the secret item successfully", func(t *testing.T) {
			nsService := new(mock.NamespaceService)
			nsService.On("GetProjectAndNamespace", ctx, project.Name, namespace.Name).
				Return(project, namespace, nil)
			defer nsService.AssertExpectations(t)

			secretRepo := new(mock.ProjectSecretRepository)
			secretRepo.On("Save", ctx, namespace, secret1).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretRepoFac := new(mock.ProjectSecretRepoFactory)
			secretRepoFac.On("New", project).Return(secretRepo)
			defer secretRepoFac.AssertExpectations(t)

			svc := service.NewSecretService(nsService, secretRepoFac)

			err := svc.Save(ctx, project.Name, namespace.Name, secret1)
			assert.Nil(t, err)
		})
	})
}
