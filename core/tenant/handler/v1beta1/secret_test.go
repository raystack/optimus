package v1beta1_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
	"github.com/odpf/optimus/core/tenant/handler/v1beta1"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func TestNewSecretsHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	proj, _ := tenant.ProjectNameFrom("test-Proj")
	ns, _ := tenant.ProjectNameFrom("test-NS")
	base64Val := base64.StdEncoding.EncodeToString([]byte("secret_val"))
	tnnt, _ := tenant.NewTenant(proj.String(), ns.String())

	t.Run("RegisterSecret", func(t *testing.T) {
		t.Run("returns error when invalid tenant", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   "",
				NamespaceName: "",
				SecretName:    "name",
				Value:         "secret",
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to register secret name")
		})
		t.Run("returns error when value to decode is empty", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   "proj",
				NamespaceName: "",
				SecretName:    "name",
				Value:         "",
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: empty value for secret: failed to register secret name")
		})
		t.Run("returns error when not able to decode value", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   "proj",
				NamespaceName: "",
				SecretName:    "name",
				Value:         "a",
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: failed to decode base64 string: failed to register secret name")
		})
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   "proj",
				NamespaceName: "",
				SecretName:    "",
				Value:         base64Val,
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: secret name is empty: failed to register secret ")
		})
		t.Run("returns error when error is returned from service", func(t *testing.T) {
			secretService := new(secretService)
			secretService.On("Save", ctx, mock.Anything, mock.Anything).
				Return(errors.New("error in saving"))
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
				Value:         base64Val,
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in "+
				"saving: failed to register secret name")
		})
		t.Run("saves the secret", func(t *testing.T) {
			secretService := new(secretService)
			secretService.On("Save", ctx, tnnt, mock.Anything).Return(nil)
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			registerReq := pb.RegisterSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
				Value:         base64Val,
			}

			_, err := handler.RegisterSecret(ctx, &registerReq)
			assert.Nil(t, err)
		})
	})
	t.Run("UpdateSecret", func(t *testing.T) {
		t.Run("returns error when invalid tenant", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			updateRequest := pb.UpdateSecretRequest{
				ProjectName:   "",
				NamespaceName: "",
				SecretName:    "name",
				Value:         "secret",
			}

			_, err := handler.UpdateSecret(ctx, &updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to update secret name")
		})
		t.Run("returns error when not able to decode value", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			updateRequest := pb.UpdateSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
				Value:         "secret",
			}

			_, err := handler.UpdateSecret(ctx, &updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: failed to decode base64 string: failed to update secret name")
		})
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			updateRequest := pb.UpdateSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "",
				Value:         base64Val,
			}

			_, err := handler.UpdateSecret(ctx, &updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: secret name is empty: failed to update secret ")
		})
		t.Run("returns error when error is returned from service", func(t *testing.T) {
			secretService := new(secretService)
			secretService.On("Update", ctx, tnnt, mock.Anything).Return(errors.New("error in update"))
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			updateRequest := pb.UpdateSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
				Value:         base64Val,
			}

			_, err := handler.UpdateSecret(ctx, &updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in update: failed "+
				"to update secret name")
		})
		t.Run("updates the secret", func(t *testing.T) {
			secretService := new(secretService)
			secretService.On("Update", ctx, tnnt, mock.Anything).Return(nil)
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			updateRequest := pb.UpdateSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
				Value:         base64Val,
			}

			_, err := handler.UpdateSecret(ctx, &updateRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("ListSecrets", func(t *testing.T) {
		t.Run("returns error when invalid tenant", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			listRequest := pb.ListSecretsRequest{
				ProjectName: "",
			}

			_, err := handler.ListSecrets(ctx, &listRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to list secrets")
		})
		t.Run("returns error when error is returned from service", func(t *testing.T) {
			secretService := new(secretService)
			projectTenant, _ := tenant.NewTenant(proj.String(), "")
			secretService.On("GetSecretsInfo", ctx, projectTenant).
				Return(nil, errors.New("error in list"))
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			listRequest := pb.ListSecretsRequest{
				ProjectName: proj.String(),
			}

			_, err := handler.ListSecrets(ctx, &listRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in list: failed to list secrets")
		})
		t.Run("list the information about the secrets", func(t *testing.T) {
			secretInfo := dto.SecretInfo{
				Name:      "secret",
				Digest:    "abcde",
				Type:      tenant.UserDefinedSecret,
				Namespace: ns.String(),
				UpdatedAt: time.Date(2022, 9, 22, 0, 0, 0, 0, time.UTC),
			}
			projectTenant, _ := tenant.NewTenant(proj.String(), "")
			secretService := new(secretService)
			secretService.On("GetSecretsInfo", ctx, projectTenant).
				Return([]*dto.SecretInfo{&secretInfo}, nil)
			defer secretService.AssertExpectations(t)

			handler := v1beta1.NewSecretsHandler(logger, secretService)

			listRequest := pb.ListSecretsRequest{
				ProjectName: proj.String(),
			}

			lst, err := handler.ListSecrets(ctx, &listRequest)
			assert.Nil(t, err)

			assert.Equal(t, 1, len(lst.Secrets))
		})
	})
	t.Run("DeleteSecret", func(t *testing.T) {
		t.Run("returns error when invalid tenant", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			deleteRequest := pb.DeleteSecretRequest{
				ProjectName:   "",
				NamespaceName: "",
				SecretName:    "name",
			}

			_, err := handler.DeleteSecret(ctx, &deleteRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity"+
				" project: project name is empty: failed to delete secret name")
		})
		t.Run("returns error when invalid secret name", func(t *testing.T) {
			secretService := new(secretService)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			deleteRequest := pb.DeleteSecretRequest{
				ProjectName:   "proj",
				NamespaceName: "test-ns",
				SecretName:    "",
			}

			_, err := handler.DeleteSecret(ctx, &deleteRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"secret: secret name is empty: failed to delete secret")
		})
		t.Run("returns error when error is returned from service", func(t *testing.T) {
			secretService := new(secretService)
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretService.On("Delete", ctx, tnnt, sn).
				Return(errors.New("error in delete"))
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			deleteRequest := pb.DeleteSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
			}

			_, err = handler.DeleteSecret(ctx, &deleteRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in delete: failed to "+
				"delete secret name")
		})
		t.Run("deletes the secrets", func(t *testing.T) {
			secretService := new(secretService)

			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretService.On("Delete", ctx, tnnt, sn).Return(nil)
			handler := v1beta1.NewSecretsHandler(logger, secretService)

			deleteRequest := pb.DeleteSecretRequest{
				ProjectName:   proj.String(),
				NamespaceName: ns.String(),
				SecretName:    "name",
			}

			_, err = handler.DeleteSecret(ctx, &deleteRequest)
			assert.Nil(t, err)
		})
	})
}

type secretService struct {
	mock.Mock
}

func (s *secretService) Save(ctx context.Context, tenant tenant.Tenant, secret *tenant.PlainTextSecret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *secretService) Update(ctx context.Context, tenant tenant.Tenant, secret *tenant.PlainTextSecret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *secretService) Delete(ctx context.Context, tenant tenant.Tenant, name tenant.SecretName) error {
	args := s.Called(ctx, tenant, name)
	return args.Error(0)
}

func (s *secretService) GetSecretsInfo(ctx context.Context, tenant tenant.Tenant) ([]*dto.SecretInfo, error) {
	args := s.Called(ctx, tenant)
	var secrets []*dto.SecretInfo
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*dto.SecretInfo)
	}
	return secrets, args.Error(1)
}
