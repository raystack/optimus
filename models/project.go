package models

import (
	"github.com/google/uuid"
)

const (
	ProjectStoragePathKey = "storagePath"
)

type ProjectSpec struct {
	ID uuid.UUID

	Name string

	// configuration for the registered projects
	// - ProjectStoragePathKey: specification store for scheduler inputs
	// suggested are gcs/s3 or similar object store
	Config map[string]string
}
