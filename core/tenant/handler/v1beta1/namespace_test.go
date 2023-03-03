package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/core/tenant/handler/v1beta1"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestNamespaceHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	projectConf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	savedProject, _ := tenant.NewProject("savedProj", projectConf)
	savedNS, _ := tenant.NewNamespace("savedNS", savedProject.Name(), map[string]string{"BUCKET": "gs://some_folder"})

	t.Run("RegisterProjectNamespace", func(t *testing.T) {
		t.Run("returns error when project name is empty", func(t *testing.T) {
			namespaceService := new(namespaceService)
			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			registerReq := pb.RegisterProjectNamespaceRequest{
				ProjectName: "",
				Namespace: &pb.NamespaceSpecification{
					Name:   "NS",
					Config: map[string]string{"BUCKET": "gs://some_folder"},
				},
			}

			_, err := handler.RegisterProjectNamespace(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: error in register namespace NS")
		})
		t.Run("returns error when name is empty", func(t *testing.T) {
			namespaceService := new(namespaceService)
			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			registerReq := pb.RegisterProjectNamespaceRequest{
				ProjectName: "proj",
				Namespace: &pb.NamespaceSpecification{
					Name:   "",
					Config: map[string]string{"BUCKET": "gs://some_folder"},
				},
			}

			_, err := handler.RegisterProjectNamespace(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"namespace: namespace name is empty: error in register namespace ")
		})
		t.Run("returns error when fails in service", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("Save", ctx, mock.Anything).Return(errors.New("error in saving"))
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			registerReq := pb.RegisterProjectNamespaceRequest{
				ProjectName: "proj",
				Namespace: &pb.NamespaceSpecification{
					Name:   "ns",
					Config: map[string]string{"BUCKET": "gs://some_folder"},
				},
			}

			_, err := handler.RegisterProjectNamespace(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in saving: error in "+
				"register namespace ns")
		})
		t.Run("saves the namespace successfully", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("Save", ctx, mock.Anything).Return(nil)
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			registerReq := pb.RegisterProjectNamespaceRequest{
				ProjectName: "proj",
				Namespace: &pb.NamespaceSpecification{
					Name:   "ns",
					Config: map[string]string{"BUCKET": "gs://some_folder"},
				},
			}

			_, err := handler.RegisterProjectNamespace(ctx, &registerReq)
			assert.Nil(t, err)
		})
	})
	t.Run("ListProjectNamespaces", func(t *testing.T) {
		t.Run("returns error when project name is empty", func(t *testing.T) {
			namespaceService := new(namespaceService)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			_, err := handler.ListProjectNamespaces(ctx, &pb.ListProjectNamespacesRequest{
				ProjectName: "",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: error in list namespaces")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("GetAll", ctx, savedProject.Name()).
				Return(nil, errors.New("unable to fetch"))
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			_, err := handler.ListProjectNamespaces(ctx, &pb.ListProjectNamespacesRequest{
				ProjectName: savedProject.Name().String(),
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = unable to fetch: error in list namespaces")
		})
		t.Run("returns the list of saved namespaces", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("GetAll", ctx, savedProject.Name()).Return([]*tenant.Namespace{savedNS}, nil)
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			nsRes, err := handler.ListProjectNamespaces(ctx, &pb.ListProjectNamespacesRequest{
				ProjectName: savedProject.Name().String(),
			})
			assert.Nil(t, err)

			assert.Equal(t, len(nsRes.Namespaces), 1)
			assert.Equal(t, nsRes.Namespaces[0].Name, savedNS.Name().String())
		})
	})
	t.Run("GetNamespace", func(t *testing.T) {
		t.Run("returns error when project name is empty", func(t *testing.T) {
			namespaceService := new(namespaceService)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			_, err := handler.GetNamespace(ctx, &pb.GetNamespaceRequest{
				ProjectName:   "",
				NamespaceName: "ns",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: error in get namespace ns")
		})
		t.Run("returns error when namespace name is empty", func(t *testing.T) {
			namespaceService := new(namespaceService)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			_, err := handler.GetNamespace(ctx, &pb.GetNamespaceRequest{
				ProjectName:   "proj",
				NamespaceName: "",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity"+
				" namespace: namespace name is empty: error in get namespace ")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("Get", ctx, savedProject.Name(), savedNS.Name()).
				Return(nil, errors.New("random error"))
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			_, err := handler.GetNamespace(ctx, &pb.GetNamespaceRequest{
				ProjectName:   "savedProj",
				NamespaceName: "savedNS",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = random error: error "+
				"in get namespace savedNS")
		})
		t.Run("returns the namespace successfully", func(t *testing.T) {
			namespaceService := new(namespaceService)
			namespaceService.On("Get", ctx, savedProject.Name(), savedNS.Name()).
				Return(savedNS, nil)
			defer namespaceService.AssertExpectations(t)

			handler := v1beta1.NewNamespaceHandler(logger, namespaceService)

			ns, err := handler.GetNamespace(ctx, &pb.GetNamespaceRequest{
				ProjectName:   "savedProj",
				NamespaceName: "savedNS",
			})
			assert.Nil(t, err)

			assert.Equal(t, savedNS.Name().String(), ns.GetNamespace().Name)
		})
	})
}

type namespaceService struct {
	mock.Mock
}

func (n *namespaceService) Save(ctx context.Context, namespace *tenant.Namespace) error {
	args := n.Called(ctx, namespace)
	return args.Error(0)
}

func (n *namespaceService) Get(ctx context.Context, name tenant.ProjectName, nsName tenant.NamespaceName) (*tenant.Namespace, error) {
	args := n.Called(ctx, name, nsName)
	var ns *tenant.Namespace
	if args.Get(0) != nil {
		ns = args.Get(0).(*tenant.Namespace)
	}
	return ns, args.Error(1)
}

func (n *namespaceService) GetAll(ctx context.Context, name tenant.ProjectName) ([]*tenant.Namespace, error) {
	args := n.Called(ctx, name)
	var nss []*tenant.Namespace
	if args.Get(0) != nil {
		nss = args.Get(0).([]*tenant.Namespace)
	}
	return nss, args.Error(1)
}
