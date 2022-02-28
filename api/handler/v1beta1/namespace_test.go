package v1beta1_test

import (
	"context"
	"testing"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestNamespaceOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("RegisterProjectNamespace", func(t *testing.T) {
		t.Run("should save a new namespace", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name:   "dev-test-namespace-1",
				Config: map[string]string{},
			}

			adapter := v1.NewAdapter(nil, nil)

			jobSvc := new(mock.JobService)
			defer jobSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Save", ctx, projectSpec.Name, namespaceSpec).Return(nil)
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				nil,
				namespaceService,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			resp, err := runtimeServiceServer.RegisterProjectNamespace(context.Background(), &namespaceRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectNamespaceResponse{
				Success: true,
				Message: "saved successfully",
			}, resp)
		})
		t.Run("should throw error if project does not exist while saving a namespace", func(t *testing.T) {
			projectName := "a-data-project"

			namespaceSpec := models.NamespaceSpec{
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"BUCKET": "gs://something",
				},
			}

			adapter := v1.NewAdapter(nil, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Save", ctx, projectName, namespaceSpec).
				Return(service.NewError("namespace", service.ErrNotFound, "project does not exist"))
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				nil,
				nil, nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			_, err := runtimeServiceServer.RegisterProjectNamespace(context.Background(), &namespaceRequest)
			assert.NotNil(t, err)
			assert.Equal(t, "rpc error: code = NotFound desc = project does not exist: not found for entity namespace: unable to store namespace", err.Error())
		})
	})

	t.Run("ListProjectNamespaces", func(t *testing.T) {
		t.Run("should read namespaces of a project", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			adapter := v1.NewAdapter(nil, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("GetAll", ctx, projectName).Return([]models.NamespaceSpec{namespaceSpec}, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			namespaceAdapted := adapter.ToNamespaceProto(namespaceSpec)
			request := pb.ListProjectNamespacesRequest{ProjectName: projectName}
			resp, err := runtimeServiceServer.ListProjectNamespaces(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, []*pb.NamespaceSpecification{namespaceAdapted}, resp.GetNamespaces())
		})
	})
}
