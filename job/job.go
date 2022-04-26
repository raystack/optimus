package job

import (
	"context"

	"github.com/odpf/optimus/models"
)

// SpecRepository represents a storage interface for Job specifications at a namespace level
type SpecRepository interface {
	Save(context.Context, models.JobSpec, string) error
	GetByName(context.Context, string) (models.JobSpec, error)
	GetAll(context.Context) ([]models.JobSpec, error)
	Delete(context.Context, string) error
}
