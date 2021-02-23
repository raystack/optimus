package models

import (
	"github.com/google/uuid"
)

const (
	ProjectStoragePathKey = "STORAGE_PATH"
	ProjectSchedulerHost  = "SCHEDULER_HOST"
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
}
