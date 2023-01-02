package tenant

import "github.com/odpf/optimus/internal/errors"

const (
	EntitySecret = "secret"

	SecretStorageKey    = "STORAGE"
	SecretSchedulerAuth = "SCHEDULER_AUTH"

	// SystemDefinedSecret TODO: get rid of system defined secrets
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

type PlainTextSecrets []*PlainTextSecret

func (p PlainTextSecrets) ToMap() map[string]string {
	secretMap := map[string]string{}
	for _, item := range p {
		secretMap[item.Name().String()] = item.Value()
	}
	return secretMap
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

	_type SecretType

	projName      ProjectName
	namespaceName string
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

func (s *Secret) ProjectName() ProjectName {
	return s.projName
}

func (s *Secret) NamespaceName() string {
	return s.namespaceName
}

func NewSecret(name string, _type SecretType, encodedValue string, projName ProjectName, nsName string) (*Secret, error) {
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

	if projName == "" {
		return nil, errors.InvalidArgument(EntitySecret, "invalid tenant details")
	}

	return &Secret{
		name:          secretName,
		encodedValue:  encodedValue,
		_type:         _type,
		projName:      projName,
		namespaceName: nsName,
	}, nil
}
