package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

const (
	//PersistDagPrefix is used to keep the dag during sync even if they are not in source repo
	PersistDagPrefix string = "_persist."
)

type Service struct {
	repo models.JobSpecRepository
}

// CreateJob constructs a Job and commits it to a storage
func (srv *Service) CreateJob(inputs models.JobInput) error {
	if err := srv.repo.Save(inputs); err != nil {
		return errors.Wrapf(err, "failed to save job: %s", inputs.Name)
	}
	return nil
}

// NewService creates a new instance of DAGService, requiring
// the necessary dependencies as arguments
func NewService(repo models.JobSpecRepository) *Service {
	return &Service{
		repo: repo,
	}
}
