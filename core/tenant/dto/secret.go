package dto

import (
	"time"
)

type SecretInfo struct {
	Name   string
	Digest string

	Namespace string

	UpdatedAt time.Time
}
