package tenant

import "github.com/odpf/optimus/internal/errors"

const (
	EntitySecret = "secret"

	SecretStorageKey    = "STORAGE"
	SecretSchedulerAuth = "SCHEDULER_AUTH"

	SystemDefinedSecret SecretType = "system"
	UserDefinedSecret   SecretType = "user"

	SecretTypeSystemDefinedPrefix = "_OPTIMUS_"
)

type SecretName string

func SecretNameFrom(name string) (SecretName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntitySecret, "secret name is empty")
	}
	return SecretName(name), nil
}

func (sn SecretName) String() string {
	return string(sn)
}

type PlainTextSecret struct {
	name  SecretName
	value string
}

func NewPlainTextSecret(name string, value string) (*PlainTextSecret, error) {
	secretName, err := SecretNameFrom(name)
	if err != nil {
		return nil, err
	}

	if value == "" {
		return nil, errors.InvalidArgument(EntitySecret, "empty secret value")
	}

	return &PlainTextSecret{
		name:  secretName,
		value: value,
	}, nil
}

func (p *PlainTextSecret) Value() string {
	return p.value
}

func (p *PlainTextSecret) Name() SecretName {
	return p.name
}

type SecretType string

func SecretTypeFromString(str string) (SecretType, error) {
	switch str {
	case UserDefinedSecret.String():
		return UserDefinedSecret, nil
	case SystemDefinedSecret.String():
		return SystemDefinedSecret, nil
	default:
		return "", errors.InvalidArgument(EntitySecret, "unknown type for secret type: "+str)
	}
}

func (s SecretType) String() string {
	return string(s)
}

type Secret struct {
	name         SecretName
	encodedValue string

	_type  SecretType
	tenant Tenant
}

func (s *Secret) Name() SecretName {
	return s.name
}

func (s *Secret) Type() SecretType {
	return s._type
}

func (s *Secret) EncodedValue() string {
	return s.encodedValue
}

func (s *Secret) Tenant() Tenant {
	return s.tenant
}

func NewSecret(name string, _type SecretType, encodedValue string, tenant Tenant) (*Secret, error) {
	secretName, err := SecretNameFrom(name)
	if err != nil {
		return nil, err
	}

	if _type != UserDefinedSecret && _type != SystemDefinedSecret {
		return nil, errors.InvalidArgument(EntitySecret, "invalid secret type")
	}

	if encodedValue == "" {
		return nil, errors.InvalidArgument(EntitySecret, "empty encoded secret")
	}

	if tenant.ProjectName() == "" {
		return nil, errors.InvalidArgument(EntitySecret, "invalid tenant details")
	}

	return &Secret{
		name:         secretName,
		encodedValue: encodedValue,
		_type:        _type,
		tenant:       tenant,
	}, nil
}
