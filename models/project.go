package models

import (
	"github.com/google/uuid"
)

type ProjectSpec struct {
	ID uuid.UUID

	Name   string
	Config map[string]string
}
