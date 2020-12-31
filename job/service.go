package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	//PersistDagPrefix is used to keep the dag during sync even if they are not in source repo
	PersistDagPrefix string = "_persist."
)

type JobSpecRepoFactory interface {
	New(models.ProjectSpec) store.JobSpecRepository
}

type Service struct {
	jobSpecRepoFactory JobSpecRepoFactory
}

// CreateJob constructs a Job and commits it to store
func (srv *Service) CreateJob(spec models.JobSpec, proj models.ProjectSpec) error {
	jobRepo := srv.jobSpecRepoFactory.New(proj)
	if err := jobRepo.Save(spec); err != nil {
		return errors.Wrapf(err, "failed to save job: %s", spec.Name)
	}
	return nil
}

// NewService creates a new instance of JobService, requiring
// the necessary dependencies as arguments
func NewService(jobSpecRepoFactory JobSpecRepoFactory) *Service {
	return &Service{
		jobSpecRepoFactory: jobSpecRepoFactory,
	}
}
