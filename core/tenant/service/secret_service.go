package service

import (
	"context"

	"github.com/goto/salt/log"
	"github.com/gtank/cryptopasta"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/core/tenant/dto"
	"github.com/goto/optimus/internal/errors"
)

const keyLength = 32

type SecretRepository interface {
	Save(ctx context.Context, secret *tenant.Secret) error
	Update(ctx context.Context, secret *tenant.Secret) error
	Get(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) (*tenant.Secret, error)
	GetAll(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.Secret, error)
	Delete(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) error
	GetSecretsInfo(ctx context.Context, projName tenant.ProjectName) ([]*dto.SecretInfo, error)
}

type SecretService struct {
	appKey *[keyLength]byte
	repo   SecretRepository

	logger log.Logger
}

func (s SecretService) Save(ctx context.Context, projName tenant.ProjectName, nsName string, secret *tenant.PlainTextSecret) error {
	if secret == nil {
		s.logger.Error("secret is nil")
		return errors.InvalidArgument(tenant.EntitySecret, "secret is not valid")
	}

	encoded, err := cryptopasta.Encrypt([]byte(secret.Value()), s.appKey)
	if err != nil {
		s.logger.Error("error encrypting secret: %s", err)
		return errors.InternalError(tenant.EntitySecret, "unable to encrypt the secret", err)
	}

	item, err := tenant.NewSecret(secret.Name().String(), string(encoded), projName, nsName)
	if err != nil {
		s.logger.Error("error encountered when constructing a new secret: %s", err)
		return err
	}

	return s.repo.Save(ctx, item)
}

func (s SecretService) Update(ctx context.Context, projName tenant.ProjectName, nsName string, secret *tenant.PlainTextSecret) error {
	if secret == nil {
		s.logger.Error("secret is nil")
		return errors.InvalidArgument(tenant.EntitySecret, "secret is not valid")
	}

	encoded, err := cryptopasta.Encrypt([]byte(secret.Value()), s.appKey)
	if err != nil {
		s.logger.Error("error encrypting secret: %s", err)
		return errors.InternalError(tenant.EntitySecret, "unable to encrypt the secret", err)
	}

	item, err := tenant.NewSecret(secret.Name().String(), string(encoded), projName, nsName)
	if err != nil {
		s.logger.Error("error constructing a new secret: %s", err)
		return err
	}

	return s.repo.Update(ctx, item)
}

func (s SecretService) Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error) {
	secretName, err := tenant.SecretNameFrom(name)
	if err != nil {
		s.logger.Error("error adapting secret name [%s]: %s", name, err)
		return nil, errors.InvalidArgument(tenant.EntitySecret, "secret name is not valid")
	}

	if projName == "" {
		s.logger.Error("project name for secret [%s] is empty")
		return nil, errors.InvalidArgument(tenant.EntitySecret, "tenant is not valid")
	}

	secret, err := s.repo.Get(ctx, projName, namespaceName, secretName)
	if err != nil {
		s.logger.Error("error getting stored secret: %s", err)
		return nil, err
	}

	cleartext, err := cryptopasta.Decrypt([]byte(secret.EncodedValue()), s.appKey)
	if err != nil {
		s.logger.Error("error decrypting secret: %s", err)
		return nil, err
	}

	return tenant.NewPlainTextSecret(secretName.String(), string(cleartext))
}

func (s SecretService) GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error) {
	if projName == "" {
		s.logger.Error("project name is empty")
		return nil, errors.InvalidArgument(tenant.EntitySecret, "project name is not valid")
	}

	secrets, err := s.repo.GetAll(ctx, projName, namespaceName)
	if err != nil {
		s.logger.Error("error getting all secrets under project [%s] namespace [%s]: %s", projName, namespaceName, err)
		return nil, err
	}

	ptsecrets := make([]*tenant.PlainTextSecret, len(secrets))
	for i, secret := range secrets {
		cleartext, err := cryptopasta.Decrypt([]byte(secret.EncodedValue()), s.appKey)
		if err != nil {
			s.logger.Error("error decrypting secret [%s]: %s", secret.Name().String(), err)
			return nil, err
		}

		pts, err := tenant.NewPlainTextSecret(secret.Name().String(), string(cleartext))
		if err != nil {
			s.logger.Error("error constructing plain text secret: %s", err)
			return nil, err
		}
		ptsecrets[i] = pts
	}

	return ptsecrets, nil
}

func (s SecretService) Delete(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) error {
	if name == "" {
		s.logger.Error("secret name is empty")
		return errors.InvalidArgument(tenant.EntitySecret, "secret name is not valid")
	}

	return s.repo.Delete(ctx, projName, nsName, name)
}

func (s SecretService) GetSecretsInfo(ctx context.Context, projName tenant.ProjectName) ([]*dto.SecretInfo, error) {
	return s.repo.GetSecretsInfo(ctx, projName)
}

func NewSecretService(appKey *[32]byte, repo SecretRepository, logger log.Logger) *SecretService {
	return &SecretService{
		appKey: appKey,
		repo:   repo,
		logger: logger,
	}
}
