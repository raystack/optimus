package models

import (
	"time"

	"github.com/google/uuid"
)

type BackupResourceRequest struct {
	Resource   ResourceSpec
	BackupSpec BackupRequest
	BackupTime time.Time
}

type BackupResourceResponse struct {
	ResultURN  string
	ResultSpec interface{}
}

type BackupRequest struct {
	ID               uuid.UUID
	ResourceName     string
	Project          ProjectSpec
	Namespace        NamespaceSpec
	Datastore        string
	Description      string
	IgnoreDownstream bool
	Config           map[string]string
	DryRun           bool
}

type BackupResult struct {
	URN  string
	Spec interface{}
}

type BackupResponse struct {
	ResourceURN string
	Result      BackupResult
}

type BackupSpec struct {
	ID          uuid.UUID
	Resource    ResourceSpec
	Result      map[string]interface{}
	Description string
	Config      map[string]string
	CreatedAt   time.Time
}
