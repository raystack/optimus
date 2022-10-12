package dto

import (
	"time"

	"github.com/odpf/optimus/core/tenant"
)

type SecretInfo struct {
	Name   string
	Digest string

	Type      tenant.SecretType
	Namespace string

	UpdatedAt time.Time
}
