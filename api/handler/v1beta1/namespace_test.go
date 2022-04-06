package v1beta1_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
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

			namespaceServiceServer := v1.NewNamespaceServiceServer(
				log,
				v1.NewAdapter(nil, nil),
				namespaceService,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			resp, err := namespaceServiceServer.RegisterProjectNamespace(ctx, &namespaceRequest)
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

			namespaceServiceServer := v1.NewNamespaceServiceServer(
				log,
				adapter,
				namespaceService,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			_, err := namespaceServiceServer.RegisterProjectNamespace(ctx, &namespaceRequest)
			assert.NotNil(t, err)
			assert.Equal(t, "rpc error: code = NotFound desc = project does not exist: not found for entity namespace: unable to store namespace", err.Error())
		})
	})

	t.Run("ListProjectNamespaces", func(t *testing.T) {
		t.Run("should read namespaces of a project", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
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

			adapter := v1.NewAdapter(nil, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("GetAll", ctx, projectName).Return([]models.NamespaceSpec{namespaceSpec}, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			namespaceServiceServer := v1.NewNamespaceServiceServer(
				log,
				adapter,
				namespaceService,
			)

			namespaceAdapted := adapter.ToNamespaceProto(namespaceSpec)
			request := pb.ListProjectNamespacesRequest{ProjectName: projectName}
			resp, err := namespaceServiceServer.ListProjectNamespaces(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, []*pb.NamespaceSpecification{namespaceAdapted}, resp.GetNamespaces())
		})
	})
}
