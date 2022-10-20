package scheduling

import (
	"github.com/google/uuid"
	"github.com/odpf/optimus/core/tenant"
	"golang.org/x/net/context"
)

type DeploymentID uuid.UUID

type DeploymentManager interface {
	Create(ctx context.Context, projectName tenant.ProjectName) (DeploymentID, error)
}
