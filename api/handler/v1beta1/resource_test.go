package v1beta1_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type ResourceServiceServerTestSuite struct {
	suite.Suite
	ctx              context.Context //nolint:containedctx
	namespaceService *mock.NamespaceService
	resourceService  *mock.DatastoreService // TODO: refactor to service package
	datastoreRepo    *mock.SupportedDatastoreRepo
	log              log.Logger
	progressObserver progress.Observer

	jobReq        *pb.DeployJobSpecificationRequest
	resourceReq   *pb.DeployResourceSpecificationRequest
	projectSpec   models.ProjectSpec
	namespaceSpec models.NamespaceSpec
}

func (s *ResourceServiceServerTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.namespaceService = new(mock.NamespaceService)
	s.datastoreRepo = new(mock.SupportedDatastoreRepo)
	s.resourceService = new(mock.DatastoreService)
	s.log = log.NewNoop()

	s.projectSpec = models.ProjectSpec{}
	s.projectSpec.Name = "project-a"
	s.projectSpec.ID = models.ProjectID(uuid.MustParse("26a0d6a0-13c6-4b30-ae6f-29233df70f31"))

	s.namespaceSpec = models.NamespaceSpec{}
	s.namespaceSpec.Name = "ns1"
	s.namespaceSpec.ID = uuid.MustParse("ceba7919-e07d-48b4-a4ce-141d79a3b59d")

	s.jobReq = &pb.DeployJobSpecificationRequest{}
	s.jobReq.ProjectName = s.projectSpec.Name
	s.jobReq.NamespaceName = s.namespaceSpec.Name

	s.resourceReq = &pb.DeployResourceSpecificationRequest{}
	s.resourceReq.DatastoreName = "datastore-1"
	s.resourceReq.ProjectName = s.projectSpec.Name
	s.resourceReq.NamespaceName = s.namespaceSpec.Name
}

func (s *ResourceServiceServerTestSuite) newResourceServiceServer() *v1.ResourceServiceServer {
	return v1.NewResourceServiceServer(
		s.log,
		s.resourceService,
		s.namespaceService,
		s.datastoreRepo,
		s.progressObserver,
	)
}

func TestResourceServiceServerTestSuite(t *testing.T) {
	s := new(ResourceServiceServerTestSuite)
	suite.Run(t, s)
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Success_NoResourceSpec() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	s.resourceService.On("UpdateResource", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything, mock2.Anything).Return(nil).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().NoError(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
	s.resourceService.AssertExpectations(s.T())
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Success_TwoResourceSpec() {
	resourceSpecs := []*pb.ResourceSpecification{}
	resourceSpecs = append(resourceSpecs, &pb.ResourceSpecification{Name: "resource-1", Type: "table"})
	resourceSpecs = append(resourceSpecs, &pb.ResourceSpecification{Name: "resource-2", Type: "table"})
	s.resourceReq.Resources = resourceSpecs
	adaptedResources := []models.ResourceSpec{}
	adaptedResources = append(adaptedResources, models.ResourceSpec{Name: "resource-1"})
	adaptedResources = append(adaptedResources, models.ResourceSpec{Name: "resource-2"})

	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)
	dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(adaptedResources[0], nil).Once()
	dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(adaptedResources[1], nil).Once()

	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

	dsTypeDatasetController := new(mock.DatastoreTypeController)
	dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable: dsTypeTableController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)
	datastorer.On("Name").Return("datastore-1")

	s.datastoreRepo.On("GetByName", "datastore-1").Return(datastorer, nil)
	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	s.resourceService.On("UpdateResource", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything, mock2.Anything).Return(nil).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().NoError(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
	s.resourceService.AssertExpectations(s.T())
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Fail_StreamRecvError() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Recv").Return(nil, errors.New("any error")).Once()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Fail_NamespaceServiceGetError() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(models.NamespaceSpec{}, errors.New("any error")).Once()
	stream.On("Send", mock2.Anything).Return(nil).Twice()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Fail_AdapterFromResourceProtoError() {
	resourceSpecs := []*pb.ResourceSpecification{}
	resourceSpecs = append(resourceSpecs, &pb.ResourceSpecification{Name: "resource-1"})
	s.resourceReq.Resources = resourceSpecs

	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

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

	s.datastoreRepo.On("GetByName", "datastore-1").Return(datastorer, nil)
	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	stream.On("Send", mock2.Anything).Return(nil).Twice()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
}

func (s *ResourceServiceServerTestSuite) TestDeployResourceSpecification_Fail_ResourceServiceUpdateResourceError() {
	stream := new(mock.DeployResourceSpecificationServer)
	stream.On("Context").Return(s.ctx)
	stream.On("Recv").Return(s.resourceReq, nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()

	s.namespaceService.On("Get", s.ctx, s.jobReq.GetProjectName(), s.jobReq.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
	s.resourceService.On("UpdateResource", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("any error")).Once()
	stream.On("Send", mock2.Anything).Return(nil).Once()

	runtimeServiceServer := s.newResourceServiceServer()
	err := runtimeServiceServer.DeployResourceSpecification(stream)

	s.Assert().Error(err)
	stream.AssertExpectations(s.T())
	s.namespaceService.AssertExpectations(s.T())
	s.resourceService.AssertExpectations(s.T())
}

func TestResourcesOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should create datastore resource successfully", func(t *testing.T) {
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

			resourceServiceServer := v1.NewResourceServiceServer(
				log,
				resourceSvc,
				namespaceService, dsRepo,
				nil,
			)

			resp, err := resourceServiceServer.CreateResource(ctx, &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})

	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should update datastore resource successfully", func(t *testing.T) {
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
			resourceSvc.On("UpdateResource", ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			resourceServiceServer := v1.NewResourceServiceServer(
				log,
				resourceSvc,
				namespaceService, dsRepo,
				nil,
			)

			resp, err := resourceServiceServer.UpdateResource(ctx, &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})
}
