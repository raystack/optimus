package v1beta1_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestRuntimeServiceServerResourceTestSuite(t *testing.T) {
	s := new(RuntimeServiceServerTestSuite)
	suite.Run(t, s)
}

func (s *RuntimeServiceServerTestSuite) TestDeployResourceSpecification_Success_NoResourceSpec() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	s.resourceService.On("UpdateResource", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything).Return(nil).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newRuntimeServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().NoError(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
	s.resourceService.AssertExpectations(s.T())
}

func (s *RuntimeServiceServerTestSuite) TestDeployResourceSpecification_Success_TwoResourceSpec() {
	resourceSpecs := []*pb.ResourceSpecification{}
	resourceSpecs = append(resourceSpecs, &pb.ResourceSpecification{Name: "resource-1"})
	resourceSpecs = append(resourceSpecs, &pb.ResourceSpecification{Name: "resource-2"})
	s.resourceReq.Resources = resourceSpecs
	adaptedResources := []models.ResourceSpec{}
	adaptedResources = append(adaptedResources, models.ResourceSpec{Name: "resource-1"})
	adaptedResources = append(adaptedResources, models.ResourceSpec{Name: "resource-2"})

	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	for i := range resourceSpecs {
		s.adapter.On("FromResourceProto", resourceSpecs[i], s.resourceReq.DatastoreName).Return(adaptedResources[i], nil).Once()
	}
	s.resourceService.On("UpdateResource", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything).Return(nil).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newRuntimeServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().NoError(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
	s.resourceService.AssertExpectations(s.T())
}

func (s *RuntimeServiceServerTestSuite) TestDeployResourceSpecification_Fail_StreamRecvError() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Recv").Return(nil, errors.New("any error")).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newRuntimeServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
}

func (s *RuntimeServiceServerTestSuite) TestDeployResourceSpecification_Fail_NamespaceServiceGetError() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(models.NamespaceSpec{}, errors.New("any error")).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newRuntimeServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
}

// TODO: refactor to test suite
func TestResourcesOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should create datastore resource successfully", func(t *testing.T) {
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

			// prepare mocked datastore
			dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

			dsTypeTableController := new(mock.DatastoreTypeController)
			dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

			dsTypeDatasetController := new(mock.DatastoreTypeController)
			dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

			dsController := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeDataset: dsTypeTableController,
			}
			datastorer := new(mock.Datastorer)
			datastorer.On("Types").Return(dsController)
			datastorer.On("Name").Return("bq")

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(resourceSpec, nil)

			req := pb.CreateResourceRequest{
				ProjectName:   projectName,
				DatastoreName: "bq",
				Resource: &pb.ResourceSpecification{
					Version: 1,
					Name:    "proj.datas",
					Type:    models.ResourceTypeDataset.String(),
				},
				NamespaceName: namespaceSpec.Name,
			}

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("CreateResource", ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				nil,
				namespaceService,
				nil,
				v1.NewAdapter(nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.CreateResource(ctx, &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})

	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should update datastore resource successfully", func(t *testing.T) {
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

			// prepare mocked datastore
			dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

			dsTypeTableController := new(mock.DatastoreTypeController)
			dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

			dsTypeDatasetController := new(mock.DatastoreTypeController)
			dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

			dsController := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeDataset: dsTypeTableController,
			}
			datastorer := new(mock.Datastorer)
			datastorer.On("Types").Return(dsController)
			datastorer.On("Name").Return("bq")

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(resourceSpec, nil)

			req := pb.UpdateResourceRequest{
				ProjectName:   projectName,
				DatastoreName: "bq",
				Resource: &pb.ResourceSpecification{
					Version: 1,
					Name:    "proj.datas",
					Type:    models.ResourceTypeDataset.String(),
				},
				NamespaceName: namespaceSpec.Name,
			}

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("UpdateResource", ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				nil,
				namespaceService,
				nil,
				v1.NewAdapter(nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.UpdateResource(ctx, &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})
}
