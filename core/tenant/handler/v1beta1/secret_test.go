package v1beta1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
)

func TestNewSecretsHandler(t *testing.T) {
	t.Run("RegisterSecret", func(t *testing.T) {

	})
	t.Run("UpdateSecret", func(t *testing.T) {

	})
	t.Run("ListSecrets", func(t *testing.T) {

	})
	t.Run("DeleteSecret", func(t *testing.T) {

	})
}

type SecretService struct {
	mock.Mock
}

func (s *SecretService) Save(ctx context.Context, tenant tenant.Tenant, secret *tenant.PlainTextSecret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *SecretService) Update(ctx context.Context, tenant tenant.Tenant, secret *tenant.PlainTextSecret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *SecretService) Delete(ctx context.Context, tenant tenant.Tenant, name string) error {
	args := s.Called(ctx, tenant, name)
	return args.Error(0)
}

func (s *SecretService) GetSecretsInfo(ctx context.Context, tenant tenant.Tenant) ([]*dto.SecretInfo, error) {
	args := s.Called(ctx, tenant)
	var secrets []*dto.SecretInfo
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*dto.SecretInfo)
	}
	return secrets, args.Error(1)
}
