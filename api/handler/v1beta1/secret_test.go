package v1beta1_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestSecretManagementOnRuntimeServer(t *testing.T) {
	ctx := context.Background()

	projectName := "a-data-project"

	projectSpec := models.ProjectSpec{
		Name: projectName,
		Config: map[string]string{
			"BUCKET": "gs://some_folder",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		ID:   uuid.New(),
		Name: "dev-test-namespace-1",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: projectSpec,
	}

	t.Run("RegisterSecret", func(t *testing.T) {
		t.Run("should return error when secret is empty", func(t *testing.T) {
			runtimeServiceServer := createTestRuntimeServiceServer(nil)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         "",
				NamespaceName: namespaceSpec.Name,
			}

			_, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.NotNil(t, err)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = empty value for secret",
				err.Error())
		})

		t.Run("should return error when secret not encoded", func(t *testing.T) {
			runtimeServiceServer := createTestRuntimeServiceServer(nil)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         "world",
				NamespaceName: namespaceSpec.Name,
			}

			_, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.NotNil(t, err)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = failed to decode base64 string: \nillegal base64 data at input byte 4",
				err.Error())
		})

		t.Run("should register a secret successfully", func(t *testing.T) {
			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			secretService := new(mock.SecretService)
			secretService.On("Save", ctx, projectSpec.Name, namespaceSpec.Name, sec).Return(nil)
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
				NamespaceName: namespaceSpec.Name,
			}

			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterSecretResponse{}, resp)
		})

		t.Run("should return error if saving to secret repository fails", func(t *testing.T) {
			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			secretService := new(mock.SecretService)
			secretService.On("Save", ctx, projectSpec.Name, namespaceSpec.Name, sec).
				Return(errors.New("error while saving secret"))
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName:   projectSpec.Name,
				NamespaceName: namespaceSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = error while saving secret: failed to register secret hello", err.Error())
		})
	})

	t.Run("UpdateSecret", func(t *testing.T) {
		t.Run("should update a secret successfully", func(t *testing.T) {
			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			secretService := new(mock.SecretService)
			secretService.On("Update", ctx, projectSpec.Name, namespaceSpec.Name, sec).Return(nil)
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.UpdateSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
				NamespaceName: namespaceSpec.Name,
			}
			resp, err := runtimeServiceServer.UpdateSecret(context.Background(), &secretRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateSecretResponse{}, resp)
		})

		t.Run("should return error if updating to secret repository fails", func(t *testing.T) {
			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			secretService := new(mock.SecretService)
			secretService.On("Update", ctx, projectSpec.Name, namespaceSpec.Name, sec).
				Return(errors.New("random error"))
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.UpdateSecretRequest{
				ProjectName:   projectSpec.Name,
				NamespaceName: namespaceSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.UpdateSecret(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to update secret hello", err.Error())
		})
	})

	t.Run("ListSecrets", func(t *testing.T) {
		t.Run("should return error when fails to get list of secrets", func(t *testing.T) {
			secretService := new(mock.SecretService)
			secretService.On("List", ctx, projectSpec.Name).
				Return([]models.SecretItemInfo{{}}, errors.New("random error"))
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.ListSecretsRequest{
				ProjectName: projectSpec.Name,
			}
			resp, err := runtimeServiceServer.ListSecrets(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to list secrets", err.Error())
		})

		t.Run("should return list of secrets", func(t *testing.T) {
			secretItems := []models.SecretItemInfo{
				{
					Name:      "MySecret",
					Digest:    "digest",
					Namespace: "",
					ID:        uuid.New(),
				},
			}
			secretService := new(mock.SecretService)
			secretService.On("List", ctx, projectSpec.Name).Return(secretItems, nil)
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.ListSecretsRequest{
				ProjectName: projectSpec.Name,
			}
			resp, err := runtimeServiceServer.ListSecrets(context.Background(), &secretRequest)
			assert.Nil(t, err)
			assert.Len(t, resp.Secrets, 1)
			assert.Equal(t, resp.Secrets[0].Name, secretItems[0].Name)
		})
	})

	t.Run("DeleteSecret", func(t *testing.T) {
		t.Run("returns error when service has error", func(t *testing.T) {
			secretService := new(mock.SecretService)
			secretService.On("Delete", ctx, projectSpec.Name, "", "hello").
				Return(errors.New("random error"))
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.DeleteSecretRequest{
				ProjectName: projectSpec.Name,
				SecretName:  "hello",
			}
			resp, err := runtimeServiceServer.DeleteSecret(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to delete secret hello", err.Error())
		})
		t.Run("deletes the secret successfully", func(t *testing.T) {
			secretService := new(mock.SecretService)
			secretService.On("Delete", ctx, projectSpec.Name, namespaceSpec.Name, "hello").
				Return(nil)
			defer secretService.AssertExpectations(t)

			runtimeServiceServer := createTestRuntimeServiceServer(secretService)

			secretRequest := pb.DeleteSecretRequest{
				ProjectName:   projectSpec.Name,
				NamespaceName: namespaceSpec.Name,
				SecretName:    "hello",
			}
			_, err := runtimeServiceServer.DeleteSecret(context.Background(), &secretRequest)
			assert.Nil(t, err)
		})
	})
}

func createTestRuntimeServiceServer(secretService service.SecretService) *v1.RuntimeServiceServer {
	return v1.NewRuntimeServiceServer(
		log.NewNoop(),
		"someVersion1.0",
		nil,
		nil,
		nil,
		nil,
		nil,
		secretService,
		nil,
		nil,
		nil,
		nil,
	)
}
