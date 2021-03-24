package models

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/google/uuid"
)

const (
	ProjectStoragePathKey = "STORAGE_PATH"
	ProjectSchedulerHost  = "SCHEDULER_HOST"

	ProjectSecretStorageKey = "STORAGE"
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
