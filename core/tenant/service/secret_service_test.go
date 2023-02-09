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
	projectName, _ := tenant.ProjectNameFrom("test-project")
	nsName := "test-namespace"
	invalidSecret := tenant.Secret{}

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Save(ctx, projectName, nsName, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret is not valid")
		})
		t.Run("returns error when secret name is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			invalidSecret := tenant.PlainTextSecret{}
			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Save(ctx, projectName, nsName, &invalidSecret)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when repo return error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Save", ctx, mock.Anything).Return(errors.New("error in save"))
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Save(ctx, projectName, nsName, sec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in save")
		})
		t.Run("saves the secret after encoding", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Save", ctx, mock.Anything).Return(nil)
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Save(ctx, projectName, nsName, sec)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Update(ctx, projectName, nsName, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret is not valid")
		})
		t.Run("returns error when secret name is not provided", func(t *testing.T) {
			secretRepo := new(secretRepo)
			invalidSecret := tenant.PlainTextSecret{}

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Update(ctx, projectName, nsName, &invalidSecret)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when repo return error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Update", ctx, mock.Anything).Return(errors.New("error in update"))
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Update(ctx, projectName, nsName, sec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update")
		})
		t.Run("saves the secret after encoding", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("Update", ctx, mock.Anything).Return(nil)
			defer secretRepo.AssertExpectations(t)

			sec, err := tenant.NewPlainTextSecret("name", "value")
			assert.Nil(t, err)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Update(ctx, projectName, nsName, sec)
			assert.Nil(t, err)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.Get(ctx, projectName, nsName, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is not valid")
		})
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.Get(ctx, "", nsName, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: tenant is not valid")
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, projectName, nsName, sn).Return(nil, errors.New("error in get"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err = secretService.Get(ctx, projectName, nsName, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in get")
		})
		t.Run("returns error when not able to decode", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, projectName, nsName, sn).Return(&invalidSecret, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err = secretService.Get(ctx, projectName, nsName, "name")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "malformed ciphertext")
		})
		t.Run("returns the secret in plain text form", func(t *testing.T) {
			encodedArr := []byte{
				63, 158, 156, 88, 23, 217, 166, 22, 135, 126, 204, 156, 107, 103, 217, 229, 58, 37,
				182, 124, 36, 80, 59, 94, 141, 238, 154, 6, 197, 70, 227, 117, 185,
			}
			sec, err := tenant.NewSecret("name", string(encodedArr), projectName, nsName)
			assert.Nil(t, err)

			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Get", ctx, projectName, nsName, sn).Return(sec, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			s, err := secretService.Get(ctx, projectName, nsName, "name")
			assert.Nil(t, err)
			assert.Equal(t, "name", s.Name().String())
			assert.Equal(t, "value", s.Value())
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, "", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: project name is not valid")
		})
		t.Run("returns error when repo returns error", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, projectName, nsName).Return(nil, errors.New("error in get all"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, projectName, nsName)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in get all")
		})
		t.Run("returns error when not able to decode", func(t *testing.T) {
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, projectName, nsName).
				Return([]*tenant.Secret{&invalidSecret}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			_, err := secretService.GetAll(ctx, projectName, nsName)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "malformed ciphertext")
		})
		t.Run("returns the secret in plain text form", func(t *testing.T) {
			encodedArr := []byte{
				63, 158, 156, 88, 23, 217, 166, 22, 135, 126, 204, 156, 107, 103, 217, 229, 58, 37,
				182, 124, 36, 80, 59, 94, 141, 238, 154, 6, 197, 70, 227, 117, 185,
			}
			sec, _ := tenant.NewSecret("name", string(encodedArr), projectName, nsName)
			secretRepo := new(secretRepo)
			secretRepo.On("GetAll", ctx, projectName, nsName).Return([]*tenant.Secret{sec}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			s, err := secretService.GetAll(ctx, projectName, nsName)
			assert.Nil(t, err)
			assert.Equal(t, "name", s[0].Name().String())
			assert.Equal(t, "value", s[0].Value())
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("returns error when secret name is empty", func(t *testing.T) {
			secretRepo := new(secretRepo)

			secretService := service.NewSecretService(key, secretRepo)
			err := secretService.Delete(ctx, projectName, nsName, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is not valid")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Delete", ctx, projectName, nsName, sn).Return(errors.New("error in delete"))
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Delete(ctx, projectName, nsName, sn)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in delete")
		})
		t.Run("deletes the secret successfully", func(t *testing.T) {
			sn, err := tenant.SecretNameFrom("name")
			assert.Nil(t, err)

			secretRepo := new(secretRepo)
			secretRepo.On("Delete", ctx, projectName, nsName, sn).Return(nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			err = secretService.Delete(ctx, projectName, nsName, "name")
			assert.Nil(t, err)
		})
	})
	t.Run("GetSecretsInfo", func(t *testing.T) {
		t.Run("returns secret info", func(t *testing.T) {
			secretInfo := dto.SecretInfo{
				Name:      "name",
				Digest:    "abcdef",
				Namespace: "namespace",
			}
			secretRepo := new(secretRepo)
			secretRepo.On("GetSecretsInfo", ctx, projectName).Return([]*dto.SecretInfo{&secretInfo}, nil)
			defer secretRepo.AssertExpectations(t)

			secretService := service.NewSecretService(key, secretRepo)
			info, err := secretService.GetSecretsInfo(ctx, projectName)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(info))
			assert.Equal(t, "name", info[0].Name)
		})
	})
}

type secretRepo struct {
	mock.Mock
}

func (s *secretRepo) Save(ctx context.Context, secret *tenant.Secret) error {
	args := s.Called(ctx, secret)
	return args.Error(0)
}

func (s *secretRepo) Update(ctx context.Context, secret *tenant.Secret) error {
	args := s.Called(ctx, secret)
	return args.Error(0)
}

func (s *secretRepo) Get(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) (*tenant.Secret, error) {
	args := s.Called(ctx, projName, nsName, name)
	var sec *tenant.Secret
	if args.Get(0) != nil {
		sec = args.Get(0).(*tenant.Secret)
	}
	return sec, args.Error(1)
}

func (s *secretRepo) GetAll(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.Secret, error) {
	args := s.Called(ctx, projName, nsName)
	var secrets []*tenant.Secret
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*tenant.Secret)
	}
	return secrets, args.Error(1)
}

func (s *secretRepo) Delete(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) error {
	args := s.Called(ctx, projName, nsName, name)
	return args.Error(0)
}

func (s *secretRepo) GetSecretsInfo(ctx context.Context, projName tenant.ProjectName) ([]*dto.SecretInfo, error) {
	args := s.Called(ctx, projName)
	var secrets []*dto.SecretInfo
	if args.Get(0) != nil {
		secrets = args.Get(0).([]*dto.SecretInfo)
	}
	return secrets, args.Error(1)
}
