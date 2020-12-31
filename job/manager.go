package job

import (
	"github.com/odpf/optimus/models"
)

type Manager struct {
}

// Deploy
func (srv *Manager) Deploy(proj models.ProjectSpec) error {
	return nil
}

// NewService creates a new instance of DAGService, requiring
// the necessary dependencies as arguments
func NewManager() *Manager {
	return &Manager{}
}
