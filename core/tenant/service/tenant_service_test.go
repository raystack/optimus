package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/service"
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
			projGetter := new(ProjectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(nil, errors.New("unable to get"))
			defer projGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nil, nil)

			_, err := tenantService.GetDetails(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "unable to get")
		})
		t.Run("returns error when unable to get namespace", func(t *testing.T) {
			projGetter := new(ProjectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			nsName, _ := tnnt.NamespaceName()
			nsGetter := new(NamespaceGetter)
			nsGetter.On("Get", ctx, tnnt.ProjectName(), nsName).Return(nil, errors.New("unable to get ns"))
			defer nsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nsGetter, nil)

			_, err := tenantService.GetDetails(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "unable to get ns")
		})
		t.Run("returns details with only project", func(t *testing.T) {
			projGetter := new(ProjectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nil, nil)

			details, err := tenantService.GetDetails(ctx, tnnt.ToProjectScope())
			assert.Nil(t, err)
			assert.Equal(t, proj.Name().String(), details.Project().Name().String())
		})
		t.Run("returns both project and namespace", func(t *testing.T) {
			projGetter := new(ProjectGetter)
			projGetter.On("Get", ctx, tnnt.ProjectName()).Return(proj, nil)
			defer projGetter.AssertExpectations(t)

			nsName, _ := tnnt.NamespaceName()
			nsGetter := new(NamespaceGetter)
			nsGetter.On("Get", ctx, tnnt.ProjectName(), nsName).Return(ns, nil)
			defer nsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(projGetter, nsGetter, nil)

			d, err := tenantService.GetDetails(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, proj.Name().String(), d.Project().Name().String())
			receivedNS, _ := d.Namespace()
			assert.Equal(t, receivedNS.Name(), ns.Name())
		})
	})
	t.Run("GetSecrets", func(t *testing.T) {
		t.Run("calls secrets getter to get all the secrets for tenant", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretsGetter := new(SecretGetter)
			secretsGetter.On("GetAll", ctx, tnnt).Return([]*tenant.PlainTextSecret{pts}, nil)
			defer secretsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(nil, nil, secretsGetter)

			secrets, err := tenantService.GetSecrets(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(secrets))
		})
	})
	t.Run("GetSecret", func(t *testing.T) {
		t.Run("calls secrets getter to get the secret for tenant", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretsGetter := new(SecretGetter)
			secretsGetter.On("Get", ctx, tnnt, "secret_name").Return(pts, nil)
			defer secretsGetter.AssertExpectations(t)

			tenantService := service.NewTenantService(nil, nil, secretsGetter)

			secret, err := tenantService.GetSecret(ctx, tnnt, pts.Name())
			assert.Nil(t, err)
			assert.Equal(t, "secret_value", secret.Value())
		})
	})
}

type ProjectGetter struct {
	mock.Mock
}

func (p *ProjectGetter) Get(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	args := p.Called(ctx, name)
	var prj *tenant.Project
	if args.Get(0) != nil {
		prj = args.Get(0).(*tenant.Project)
	}
	return prj, args.Error(1)
}

type NamespaceGetter struct {
	mock.Mock
}

func (n *NamespaceGetter) Get(ctx context.Context, prjName tenant.ProjectName, nsName tenant.NamespaceName) (*tenant.Namespace, error) {
	args := n.Called(ctx, prjName, nsName)
	var ns *tenant.Namespace
	if args.Get(0) != nil {
		ns = args.Get(0).(*tenant.Namespace)
	}
	return ns, args.Error(1)
}

type SecretGetter struct {
	mock.Mock
}

func (s *SecretGetter) Get(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, ten, name)
	var pts *tenant.PlainTextSecret
	if args.Get(0) != nil {
		pts = args.Get(0).(*tenant.PlainTextSecret)
	}
	return pts, args.Error(1)
}

func (s *SecretGetter) GetAll(ctx context.Context, ten tenant.Tenant) ([]*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, ten)
	var ptss []*tenant.PlainTextSecret
	if args.Get(0) != nil {
		ptss = args.Get(0).([]*tenant.PlainTextSecret)
	}
	return ptss, args.Error(1)
}
