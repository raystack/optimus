package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
	"github.com/odpf/optimus/core/tenant/service"
)

func TestSecretService(t *testing.T) {
	ctx := context.Background()
	bytes := []byte("32charshtesthashtesthashtesthash")
	key := (*[32]byte)(bytes[:])
	tnnt, _ := tenant.NewTenant("test-project", "test-namespace")
	invalidSecret := tenant.Secret{}

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Save(ctx, tnnt, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret is not valid")
		})
		t.Run("returns error when secret name is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			invalidSecret := tenant.PlainTextSecret{}
			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Save(ctx, tnnt, &invalidSecret)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when repo return error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Save", ctx, tnnt, mock.Anything).Return(errors.New("error in save"))
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Save(ctx, tnnt, sec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in save")
		})
		t.Run("saves the secret after encoding", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Save", ctx, tnnt, mock.Anything).Return(nil)
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Save(ctx, tnnt, sec)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Update(ctx, tnnt, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret is not valid")
		})
		t.Run("returns error when secret name is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)
			invalidSecret := tenant.PlainTextSecret{}

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Update(ctx, tnnt, &invalidSecret)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when repo return error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Update", ctx, tnnt, mock.Anything).Return(errors.New("error in update"))
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Update(ctx, tnnt, sec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update")
		})
		t.Run("saves the secret after encoding", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Update", ctx, tnnt, mock.Anything).Return(nil)
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Update(ctx, tnnt, sec)
			assert.Nil(t, err)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.Get(ctx, tnnt, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is not valid")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.Get(ctx, tenant.Tenant{}, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: tenant is not valid")
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, tnnt, sn).Return(nil, errors.New("error in get"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err = secretService.Get(ctx, tnnt, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in get")
		})
		t.Run("returns error when not able to decode", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, tnnt, sn).Return(&invalidSecret, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err = secretService.Get(ctx, tnnt, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "malformed ciphertext")
		})
		t.Run("returns the secret in plain text form", func(t *testing.T) {
			encodedArr := []byte{63, 158, 156, 88, 23, 217, 166, 22, 135, 126, 204, 156, 107, 103, 217, 229, 58, 37,
				182, 124, 36, 80, 59, 94, 141, 238, 154, 6, 197, 70, 227, 117, 185}
			sec, err := tenant.NewSecret("name", tenant.UserDefinedSecret, string(encodedArr), tnnt)
			assert.Nil(t, err)

			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, tnnt, sn).Return(sec, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			s, err := secretService.Get(ctx, tnnt, "name")
			assert.Nil(t, err)
			assert.Equal(t, "name", s.Name().String())
			assert.Equal(t, "value", s.Value())
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: tenant is not valid")
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, tnnt).Return(nil, errors.New("error in get all"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in get all")
		})
		t.Run("returns error when not able to decode", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, tnnt).
				Return([]*tenant.Secret{&invalidSecret}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, tnnt)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "malformed ciphertext")
		})
		t.Run("returns the secret in plain text form", func(t *testing.T) {
			encodedArr := []byte{63, 158, 156, 88, 23, 217, 166, 22, 135, 126, 204, 156, 107, 103, 217, 229, 58, 37,
				182, 124, 36, 80, 59, 94, 141, 238, 154, 6, 197, 70, 227, 117, 185}
			sec, _ := tenant.NewSecret("name", tenant.UserDefinedSecret, string(encodedArr), tnnt)
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, tnnt).Return([]*tenant.Secret{sec}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			s, err := secretService.GetAll(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, "name", s[0].Name().String())
			assert.Equal(t, "value", s[0].Value())
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Delete(ctx, tnnt, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is not valid")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Delete", ctx, tnnt, sn).Return(errors.New("error in delete"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Delete(ctx, tnnt, sn)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in delete")
		})
		t.Run("deletes the secret successfully", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Delete", ctx, tnnt, sn).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Delete(ctx, tnnt, "name")
			assert.Nil(t, err)
		})
	})
	t.Run("GetSecretsInfo", func(t *testing.T) {
		t.Run("returns secret info", func(t *testing.T) {
			secretInfo := dto.SecretInfo{
				Name:      "name",
				Digest:    "abcdef",
				Type:      tenant.UserDefinedSecret,
				Namespace: "namespace",
			}
			secretRepo := new(secretRepo)
			secretRepo.On("GetSecretsInfo", ctx, tnnt).Return([]*dto.SecretInfo{&secretInfo}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			info, err := secretService.GetSecretsInfo(ctx, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(info))
			assert.Equal(t, "name", info[0].Name)
		})
	})
}

type secretRepo struct {
	mock.Mock
}

func (s *secretRepo) Save(ctx context.Context, tenant tenant.Tenant, secret *tenant.Secret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *secretRepo) Update(ctx context.Context, tenant tenant.Tenant, secret *tenant.Secret) error {
	args := s.Called(ctx, tenant, secret)
	return args.Error(0)
}

func (s *secretRepo) Get(ctx context.Context, t tenant.Tenant, name tenant.SecretName) (*tenant.Secret, error) {
	args := s.Called(ctx, t, name)
	var sec *tenant.Secret
	if args.Get(0) != nil {
		sec = args.Get(0).(*tenant.Secret)
	}
	return sec, args.Error(1)
}

func (s *secretRepo) GetAll(ctx context.Context, t tenant.Tenant) ([]*tenant.Secret, error) {
	args := s.Called(ctx, t)
	var secrets []*tenant.Secret
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*tenant.Secret)
	}
	return secrets, args.Error(1)
}

func (s *secretRepo) Delete(ctx context.Context, tenant tenant.Tenant, name tenant.SecretName) error {
	args := s.Called(ctx, tenant, name)
	return args.Error(0)
}

func (s *secretRepo) GetSecretsInfo(ctx context.Context, t tenant.Tenant) ([]*dto.SecretInfo, error) {
	args := s.Called(ctx, t)
	var secrets []*dto.SecretInfo
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*dto.SecretInfo)
	}
	return secrets, args.Error(1)
}
