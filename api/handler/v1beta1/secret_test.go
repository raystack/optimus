package v1beta1_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestSecretManagementOnRuntimeServer(t *testing.T) {
	noop := log.NewNoop()
	ctx := context.Background()

	projectName := "a-data-project"

	projectSpec := models.ProjectSpec{
		Name: projectName,
		Config: map[string]string{
			"BUCKET": "gs://some_folder",
		},
	}
	projectRepository := new(mock.ProjectRepository)
	projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)

	projectRepoFactory := new(mock.ProjectRepoFactory)
	projectRepoFactory.On("New").Return(projectRepository)

	namespaceSpec := models.NamespaceSpec{
		ID:   uuid.New(),
		Name: "dev-test-namespace-1",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: projectSpec,
	}

	namespaceRepository := new(mock.NamespaceRepository)
	namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

	namespaceRepoFactory := new(mock.NamespaceRepoFactory)
	namespaceRepoFactory.On("New", projectSpec).Return(namespaceRepository)

	t.Run("RegisterSecret", func(t *testing.T) {
		t.Run("should return error when secret is empty", func(t *testing.T) {
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
			)

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
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
			)

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
			defer projectRepository.AssertExpectations(t)
			defer projectRepoFactory.AssertExpectations(t)
			defer namespaceRepository.AssertExpectations(t)
			defer namespaceRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", ctx, sec).Return(nil)
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec, namespaceSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFactory,
				projectSecretRepoFactory,
				nil,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
				NamespaceName: namespaceSpec.Name,
			}

			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterSecretResponse{
				Success: true,
			}, resp)
		})

		t.Run("should return error if saving to secret repository fails", func(t *testing.T) {
			defer projectRepository.AssertExpectations(t)
			defer projectRepoFactory.AssertExpectations(t)
			defer namespaceRepository.AssertExpectations(t)
			defer namespaceRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", ctx, sec).Return(errors.New("random error"))
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec, models.NamespaceSpec{}).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil, nil, nil,
				projectRepoFactory,
				namespaceRepoFactory,
				projectSecretRepoFactory,
				nil,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName: projectSpec.Name,
				SecretName:  "hello",
				Value:       base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to register secret hello", err.Error())
		})
	})

	t.Run("UpdateSecret", func(t *testing.T) {
		t.Run("should update a secret successfully", func(t *testing.T) {
			defer projectRepository.AssertExpectations(t)
			defer projectRepoFactory.AssertExpectations(t)
			defer namespaceRepository.AssertExpectations(t)
			defer namespaceRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Update", ctx, sec).Return(nil)
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec, namespaceSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil, nil, nil,
				projectRepoFactory,
				namespaceRepoFactory,
				projectSecretRepoFactory,
				nil,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.UpdateSecretRequest{
				ProjectName:   projectSpec.Name,
				SecretName:    "hello",
				Value:         base64.StdEncoding.EncodeToString([]byte("world")),
				NamespaceName: namespaceSpec.Name,
			}
			resp, err := runtimeServiceServer.UpdateSecret(context.Background(), &secretRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateSecretResponse{
				Success: true,
			}, resp)
		})

		t.Run("should return error if updating to secret repository fails", func(t *testing.T) {
			defer projectRepository.AssertExpectations(t)
			defer projectRepoFactory.AssertExpectations(t)
			defer namespaceRepository.AssertExpectations(t)
			defer namespaceRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Update", ctx, sec).Return(errors.New("random error"))
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec, models.NamespaceSpec{}).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				noop,
				"someVersion1.0",
				nil, nil, nil,
				projectRepoFactory,
				namespaceRepoFactory,
				projectSecretRepoFactory,
				nil,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.UpdateSecretRequest{
				ProjectName: projectSpec.Name,
				SecretName:  "hello",
				Value:       base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.UpdateSecret(context.Background(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to update secret hello", err.Error())
		})
	})
}
