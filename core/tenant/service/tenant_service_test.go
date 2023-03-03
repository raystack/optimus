package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/core/tenant/service"
)

func TestTenantService(t *testing.T) {
	ctx := context.Background()
	conf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	proj, _ := tenant.NewProject("testProj", conf)
	ns, _ := tenant.NewNamespace("testNS", proj.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(proj.Name().String(), ns.Name().String())

	t.Run("GetDetails", func(t *testing.T) {
		t.Run("returns error when tenant invalid", func(t *testing.T) {
			tenantService := service.NewTenantService(nil, nil, nil)

			_, err := tenantService.GetDetails(ctx, tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: invalid tenant details provided")
		})
		t.Run("returns error when unable to get project", func(t *testing.T) {
			projGetter := new(projectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(nil, errors.New("unable to get"))
			defer projGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nil, nil)

			_, err := tenantService.GetDetails(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "unable to get")
		})
		t.Run("returns error when unable to get namespace", func(t *testing.T) {
			projGetter := new(projectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			nsGetter := new(namespaceGetter)
			nsGetter.On("Get", ctx, tnnt.ProjectName(), tnnt.NamespaceName()).Return(nil, errors.New("unable to get ns"))
			defer nsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nsGetter, nil)

			_, err := tenantService.GetDetails(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "unable to get ns")
		})
		t.Run("returns error when unable to get secrets", func(t *testing.T) {
			projGetter := new(projectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			nsGetter := new(namespaceGetter)
			nsGetter.On("Get", ctx, tnnt.ProjectName(), tnnt.NamespaceName()).Return(ns, nil)
			defer nsGetter.AssertExpectations(t)

			secGetter := new(secretGetter)
			secGetter.On("GetAll", ctx, tnnt.ProjectName(), tnnt.NamespaceName().String()).Return(nil, errors.New("unable to get secrets"))
			defer secGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nsGetter, secGetter)

			_, err := tenantService.GetDetails(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "unable to get secrets")
		})
		t.Run("returns project, namespace and secrets", func(t *testing.T) {
			projGetter := new(projectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			nsGetter := new(namespaceGetter)
			nsGetter.On("Get", ctx, tnnt.ProjectName(), tnnt.NamespaceName()).Return(ns, nil)
			defer nsGetter.AssertExpectations(t)

			pts, _ := tenant.NewPlainTextSecret("key1", "value1")
			secGetter := new(secretGetter)
			secGetter.On("GetAll", ctx, tnnt.ProjectName(), tnnt.NamespaceName().String()).
				Return([]*tenant.PlainTextSecret{pts}, nil)
			defer secGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nsGetter, secGetter)

			d, err := tenantService.GetDetails(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, proj.Name().String(), d.Project().Name().String())
			receivedNS := d.Namespace()
			assert.Equal(t, receivedNS.Name(), ns.Name())
			sec := d.SecretsMap()
			assert.Equal(t, 1, len(sec))
			assert.Equal(t, "value1", sec[pts.Name().String()])
		})
	})
	t.Run("GetProject", func(t *testing.T) {
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			projGetter := new(projectGetter)

			tenantService := service.NewTenantService(projGetter, nil, nil)

			_, err := tenantService.GetProject(ctx, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: invalid project name")
		})
		t.Run("returns the project", func(t *testing.T) {
			projGetter := new(projectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nil, nil)

			p, err := tenantService.GetProject(ctx, tnnt.ProjectName())
			assert.Nil(t, err)

			assert.Equal(t, proj.Name().String(), p.Name().String())
		})
	})
	t.Run("GetSecrets", func(t *testing.T) {
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			secretsGetter := new(secretGetter)
			tenantService := service.NewTenantService(nil, nil, secretsGetter)
			invalidTenant := tenant.Tenant{}

			_, err := tenantService.GetSecrets(ctx, invalidTenant)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: tenant is invalid")
		})
		t.Run("calls secrets getter to get all the secrets for tenant", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretsGetter := new(secretGetter)
			secretsGetter.On("GetAll", ctx, proj.Name(), ns.Name().String()).Return([]*tenant.PlainTextSecret{pts}, nil)
			defer secretsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(nil, nil, secretsGetter)

			secrets, err := tenantService.GetSecrets(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(secrets))
		})
	})
	t.Run("GetSecret", func(t *testing.T) {
		t.Run("return error when project name is invalid", func(t *testing.T) {
			secretsGetter := new(secretGetter)
			tenantService := service.NewTenantService(nil, nil, secretsGetter)
			invalidTenant := tenant.Tenant{}

			_, err := tenantService.GetSecret(ctx, invalidTenant, "secret")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: tenant is invalid")
		})
		t.Run("calls secrets getter to get the secret for tenant", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretsGetter := new(secretGetter)
			secretsGetter.On("Get", ctx, proj.Name(), ns.Name().String(), "secret_name").Return(pts, nil)
			defer secretsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(nil, nil, secretsGetter)

			secret, err := tenantService.GetSecret(ctx, tnnt, pts.Name().String())
			assert.Nil(t, err)
			assert.Equal(t, "secret_value", secret.Value())
		})
	})
}

type projectGetter struct {
	mock.Mock
}

func (p *projectGetter) Get(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	args := p.Called(ctx, name)
	var prj *tenant.Project
	if args.Get(0) != nil {
		prj = args.Get(0).(*tenant.Project)
	}
	return prj, args.Error(1)
}

type namespaceGetter struct {
	mock.Mock
}

func (n *namespaceGetter) Get(ctx context.Context, prjName tenant.ProjectName, nsName tenant.NamespaceName) (*tenant.Namespace, error) {
	args := n.Called(ctx, prjName, nsName)
	var ns *tenant.Namespace
	if args.Get(0) != nil {
		ns = args.Get(0).(*tenant.Namespace)
	}
	return ns, args.Error(1)
}

type secretGetter struct {
	mock.Mock
}

func (s *secretGetter) Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, projName, namespaceName, name)
	var pts *tenant.PlainTextSecret
	if args.Get(0) != nil {
		pts = args.Get(0).(*tenant.PlainTextSecret)
	}
	return pts, args.Error(1)
}

func (s *secretGetter) GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, projName, namespaceName)
	var ptss []*tenant.PlainTextSecret
	if args.Get(0) != nil {
		ptss = args.Get(0).([]*tenant.PlainTextSecret)
	}
	return ptss, args.Error(1)
}
