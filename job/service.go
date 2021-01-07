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

// DependencyResolver compiles static and runtime dependencies
type DependencyResolver interface {
	Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error)
}

// JobSpecRepoFactory is used to store job specs
type JobSpecRepoFactory interface {
	New(models.ProjectSpec) store.JobSpecRepository
}

type Service struct {
	jobSpecRepoFactory JobSpecRepoFactory
	compiler           models.JobCompiler
	dagRepo            store.JobRepository
	dependencyResolver DependencyResolver
}

// CreateJob constructs a Job and commits it to store
func (srv *Service) CreateJob(spec models.JobSpec, proj models.ProjectSpec) error {
	jobRepo := srv.jobSpecRepoFactory.New(proj)
	if err := jobRepo.Save(spec); err != nil {
		return errors.Wrapf(err, "failed to save job: %s", spec.Name)
	}
	return nil
}

func (srv *Service) Upload(proj models.ProjectSpec) error {
	jobSpecs, err := srv.jobSpecRepoFactory.New(proj).GetAll()
	if err != nil {
		return errors.Wrapf(err, "failed to retrive jobs")
	}

	jobSpecs, err = srv.dependencyResolver.Resolve(jobSpecs)
	if err != nil {
		return err
	}

	// upload all specs
	for _, jobSpec := range jobSpecs {
		compiledJob, err := srv.compiler.Compile(jobSpec)
		if err != nil {
			return err
		}

		if err = srv.dagRepo.Save(compiledJob); err != nil {
			return err
		}
	}

	return nil
}

// NewService creates a new instance of JobService, requiring
// the necessary dependencies as arguments
func NewService(jobSpecRepoFactory JobSpecRepoFactory, dagRepo store.JobRepository,
	compiler models.JobCompiler, dependencyResolver DependencyResolver,
) *Service {
	return &Service{
		jobSpecRepoFactory: jobSpecRepoFactory,
		compiler:           compiler,
		dagRepo:            dagRepo,
		dependencyResolver: dependencyResolver,
	}
}
