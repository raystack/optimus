package service

import (
	"context"

	"github.com/gtank/cryptopasta"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
	"github.com/odpf/optimus/internal/errors"
)

const keyLength = 32

type SecretRepository interface {
	Save(context.Context, tenant.Tenant, *tenant.Secret) error
	Update(context.Context, tenant.Tenant, *tenant.Secret) error
	Get(context.Context, tenant.Tenant, tenant.SecretName) (*tenant.Secret, error)
	GetAll(context.Context, tenant.Tenant) ([]*tenant.Secret, error)
	Delete(context.Context, tenant.Tenant, tenant.SecretName) error
	GetSecretsInfo(context.Context, tenant.Tenant) ([]*dto.SecretInfo, error)
}

type SecretService struct {
	appKey *[keyLength]byte
	repo   SecretRepository
}

func (s SecretService) Save(ctx context.Context, t tenant.Tenant, secret *tenant.PlainTextSecret) error {
	if secret == nil {
		return errors.InvalidArgument(tenant.EntitySecret, "secret is not valid")
	}

	encoded, err := cryptopasta.Encrypt([]byte(secret.Value()), s.appKey)
	if err != nil {
		return errors.InternalError(tenant.EntitySecret, "unable to encrypt the secret", err)
	}

	item, err := tenant.NewSecret(secret.Name().String(), tenant.UserDefinedSecret, string(encoded), t)
	if err != nil {
		return err
	}

	return s.repo.Save(ctx, t, item)
}

func (s SecretService) Update(ctx context.Context, t tenant.Tenant, secret *tenant.PlainTextSecret) error {
	if secret == nil {
		return errors.InvalidArgument(tenant.EntitySecret, "secret is not valid")
	}

	encoded, err := cryptopasta.Encrypt([]byte(secret.Value()), s.appKey)
	if err != nil {
		return errors.InternalError(tenant.EntitySecret, "unable to encrypt the secret", err)
	}

	item, err := tenant.NewSecret(secret.Name().String(), tenant.UserDefinedSecret, string(encoded), t)
	if err != nil {
		return err
	}

	return s.repo.Update(ctx, t, item)
}

func (s SecretService) Get(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	secretName, err := tenant.SecretNameFrom(name)
	if err != nil {
		return nil, errors.InvalidArgument(tenant.EntitySecret, "secret name is not valid")
	}

	if ten.ProjectName() == "" {
		return nil, errors.InvalidArgument(tenant.EntitySecret, "tenant is not valid")
	}

	secret, err := s.repo.Get(ctx, ten, secretName)
	if err != nil {
		return nil, err
	}

	cleartext, err := cryptopasta.Decrypt([]byte(secret.EncodedValue()), s.appKey)
	if err != nil {
		return nil, err
	}

	return tenant.NewPlainTextSecret(name, string(cleartext))
}

func (s SecretService) GetAll(ctx context.Context, ten tenant.Tenant) ([]*tenant.PlainTextSecret, error) {
	if ten.ProjectName() == "" {
		return nil, errors.InvalidArgument(tenant.EntitySecret, "tenant is not valid")
	}

	secrets, err := s.repo.GetAll(ctx, ten)
	if err != nil {
		return nil, err
	}

	ptsecrets := make([]*tenant.PlainTextSecret, len(secrets))
	for i, secret := range secrets {
		cleartext, err := cryptopasta.Decrypt([]byte(secret.EncodedValue()), s.appKey)
		if err != nil {
			return nil, err
		}

		pts, err := tenant.NewPlainTextSecret(secret.Name().String(), string(cleartext))
		if err != nil {
			return nil, err
		}
		ptsecrets[i] = pts
	}

	return ptsecrets, nil
}

func (s SecretService) Delete(ctx context.Context, t tenant.Tenant, name tenant.SecretName) error {
	if name == "" {
		return errors.InvalidArgument(tenant.EntitySecret, "secret name is not valid")
	}

	return s.repo.Delete(ctx, t, name)
}

func (s SecretService) GetSecretsInfo(ctx context.Context, t tenant.Tenant) ([]*dto.SecretInfo, error) {
	return s.repo.GetSecretsInfo(ctx, t)
}

func NewSecretService(appKey *[32]byte, repo SecretRepository) *SecretService {
	return &SecretService{
		appKey: appKey,
		repo:   repo,
	}
}
