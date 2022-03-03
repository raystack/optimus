package models

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	ProjectStoragePathKey = "STORAGE_PATH"
	ProjectSchedulerHost  = "SCHEDULER_HOST"

	// Secret used for uploading prepared scheduler specifications to cloud
	// e.g. for gcs it will be base64 encoded service account for the bucket
	ProjectSecretStorageKey = "STORAGE"

	// Secret used to authenticate with scheduler provided at ProjectSchedulerHost
	ProjectSchedulerAuth = "SCHEDULER_AUTH"

	SecretTypeSystemDefined SecretType = "system"
	SecretTypeUserDefined   SecretType = "user"

	// Secret name with this prefix indicates managed by system
	SecretTypeSystemDefinedPrefix = "_OPTIMUS_"
)

var (
	// PluginSecretString generates plugin secret identifier using its type
	// and name, e.g. task, bq2bq
	PluginSecretString = func(pluginType InstanceType, pluginName string) string {
		return strings.ToUpper(fmt.Sprintf("%s_%s", pluginType, pluginName))
	}
)

type ProjectSpec struct {
	ID uuid.UUID

	Name string

	// configuration for the registered projects
	// - ProjectStoragePathKey: specification store for scheduler inputs
	// suggested are gcs/s3 or similar object store
	// - ProjectSchedulerHost: host url to connect with the scheduler used by
	// the tenant
	Config map[string]string

	// Secret contains key value pair for project level credentials and gets
	// shared with plugins(task/hook) for execution.
	// Few credentials are mandatory to be defined like ProjectSecretStorageKey
	// and few are optional as needed.
	// Plugin level secrets should be created with a convention of
	// Name: <plugintype>_<plugin_name>, Value: <base64encodedstring>
	// For example: TASK_BQ2BQ: secret_as_base64
	Secret ProjectSecrets
}

func (s ProjectSpec) String() string {
	return fmt.Sprintf("%s, %v", s.Name, s.Config)
}

type ProjectSecrets []ProjectSecretItem

func (s ProjectSecrets) String() string {
	return "*redacted*"
}

func (s ProjectSecrets) GetByName(name string) (string, bool) {
	for _, v := range s {
		if v.Name == name {
			return v.Value, true
		}
	}
	return "", false
}

type ProjectSecretItem struct {
	ID uuid.UUID

	Name  string
	Value string

	Type SecretType
}

type SecretType string

func (s SecretType) String() string {
	return string(s)
}

type ApplicationKey struct {
	key *[32]byte
}

func NewApplicationSecret(k string) (ApplicationKey, error) {
	secret := ApplicationKey{
		key: &[32]byte{},
	}
	if len(k) < 32 {
		return secret, errors.New("random hash should be 32 chars in length")
	}
	_, err := io.ReadFull(bytes.NewBufferString(k), secret.key[:])
	return secret, err
}

func (s *ApplicationKey) GetKey() *[32]byte {
	return s.key
}

type SecretItemInfo struct {
	ID     uuid.UUID
	Name   string
	Digest string

	Type      SecretType
	Namespace string
	UpdatedAt time.Time
}
