package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/core/tenant/service"
)

func TestNamespaceService(t *testing.T) {
	ctx := context.Background()
	conf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	savedProject, _ := tenant.NewProject("savedProj", conf)
	savedNS, _ := tenant.NewNamespace("savedNS", savedProject.Name(), map[string]string{})

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when fails in service", func(t *testing.T) {
			nsRepo := new(namespaceRepo)
			nsRepo.On("Save", ctx, mock.Anything).Return(errors.New("error in saving"))
			defer nsRepo.AssertExpectations(t)

			toSaveNS, _ := tenant.NewNamespace("ns", savedProject.Name(), map[string]string{"BUCKET": "gs://some_folder"})

			namespaceService := service.NewNamespaceService(nsRepo)
			err := namespaceService.Save(ctx, toSaveNS)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in saving")
		})
		t.Run("saves the project successfully", func(t *testing.T) {
			nsRepo := new(namespaceRepo)
			nsRepo.On("Save", ctx, mock.Anything).Return(nil)
			defer nsRepo.AssertExpectations(t)

			toSaveNS, _ := tenant.NewNamespace("ns", savedProject.Name(), map[string]string{"BUCKET": "gs://some_folder"})

			namespaceService := service.NewNamespaceService(nsRepo)
			err := namespaceService.Save(ctx, toSaveNS)

			assert.Nil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			namespaceRepo := new(namespaceRepo)
			namespaceRepo.On("GetAll", ctx, savedProject.Name()).
				Return(nil, errors.New("error in getting all"))
			defer namespaceRepo.AssertExpectations(t)

			namespaceService := service.NewNamespaceService(namespaceRepo)
			_, err := namespaceService.GetAll(ctx, savedProject.Name())

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting all")
		})
		t.Run("returns the list of saved projects", func(t *testing.T) {
			namespaceRepo := new(namespaceRepo)
			namespaceRepo.On("GetAll", ctx, savedProject.Name()).
				Return([]*tenant.Namespace{savedNS}, nil)
			defer namespaceRepo.AssertExpectations(t)

			namespaceService := service.NewNamespaceService(namespaceRepo)
			projs, err := namespaceService.GetAll(ctx, savedProject.Name())

			assert.Nil(t, err)
			assert.Equal(t, 1, len(projs))
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			namespaceRepo := new(namespaceRepo)
			namespaceRepo.On("GetByName", ctx, savedProject.Name(), savedNS.Name()).
				Return(nil, errors.New("error in getting"))
			defer namespaceRepo.AssertExpectations(t)

			namespaceService := service.NewNamespaceService(namespaceRepo)
			_, err := namespaceService.Get(ctx, savedProject.Name(), savedNS.Name())

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting")
		})
		t.Run("returns the project successfully", func(t *testing.T) {
			namespaceRepo := new(namespaceRepo)
			namespaceRepo.On("GetByName", ctx, savedProject.Name(), savedNS.Name()).Return(savedNS, nil)
			defer namespaceRepo.AssertExpectations(t)

			namespaceService := service.NewNamespaceService(namespaceRepo)
			ns, err := namespaceService.Get(ctx, savedProject.Name(), savedNS.Name())

			assert.Nil(t, err)
			assert.Equal(t, savedNS.Name(), ns.Name())
		})
	})
}

type namespaceRepo struct {
	mock.Mock
}

func (nr *namespaceRepo) Save(ctx context.Context, namespace *tenant.Namespace) error {
	args := nr.Called(ctx, namespace)
	return args.Error(0)
}

func (nr *namespaceRepo) GetByName(ctx context.Context, prjName tenant.ProjectName, nsName tenant.NamespaceName) (*tenant.Namespace, error) {
	args := nr.Called(ctx, prjName, nsName)
	var ns *tenant.Namespace
	if args.Get(0) != nil {
		ns = args.Get(0).(*tenant.Namespace)
	}
	return ns, args.Error(1)
}

func (nr *namespaceRepo) GetAll(ctx context.Context, name tenant.ProjectName) ([]*tenant.Namespace, error) {
	args := nr.Called(ctx, name)
	var nss []*tenant.Namespace
	if args.Get(0) != nil {
		nss = args.Get(0).([]*tenant.Namespace)
	}
	return nss, args.Error(1)
}
