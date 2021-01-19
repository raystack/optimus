package job_test

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDAGService(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create a new JobSpec and store in repository", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
			}
			projSpec := models.ProjectSpec{
				Name: "proj",
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", jobSpec).Return(nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", projSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, nil, nil)
			err := svc.Create(jobSpec, projSpec)
			assert.Nil(t, err)
		})
		t.Run("should fail if saving to repo fails", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
			}
			projSpec := models.ProjectSpec{
				Name: "proj",
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", jobSpec).Return(errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", projSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, nil, nil)
			err := svc.Create(jobSpec, projSpec)
			assert.NotNil(t, err)
		})
	})
	t.Run("Sync", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}
		t.Run("should successfully store job specs for the requested project", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name:     "test",
					Contents: []byte(`come string`),
				},
			}

			// used to store raw job specs
			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("GetAll").Return(jobSpecsBase, nil)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			jobSpecRepoFac.On("New", projSpec).Return(jobSpecRepo)
			defer jobSpecRepoFac.AssertExpectations(t)

			// used to store compiled job specs
			jobRepo := new(mock.JobRepository)
			jobRepo.On("GetAll").Return(jobs, nil)
			defer jobRepo.AssertExpectations(t)

			jobRepoFac := new(mock.JobRepoFactory)
			jobRepoFac.On("New", projSpec).Return(jobRepo, nil)
			defer jobRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", jobSpecsBase).Return(jobSpecsAfterDepenResolve, nil)
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", jobSpecsAfterDepenResolve).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			compiler := new(mock.Compiler)
			defer compiler.AssertExpectations(t)

			// compile to dag and save
			for idx, compiledJob := range jobs {
				compiler.On("Compile", jobSpecsAfterPriorityResolve[idx], projSpec).Return(compiledJob, nil)
				jobRepo.On("Save", compiledJob).Return(nil)
			}

			svc := job.NewService(jobSpecRepoFac, jobRepoFac, compiler, depenResolver, priorityResolver)
			err := svc.Sync(projSpec, nil)
			assert.Nil(t, err)
		})
		t.Run("should delete job specs from target store if there are existing specs that are no longer present in job specs", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name:     "test",
					Contents: []byte(`some string`),
				},
				{
					Name:     "test2",
					Contents: []byte(`some string`),
				},
			}

			// used to store raw job specs
			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			// used to store compiled job specs
			jobRepo := new(mock.JobRepository)
			defer jobRepo.AssertExpectations(t)

			jobRepoFac := new(mock.JobRepoFactory)
			defer jobRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			compiler := new(mock.Compiler)
			defer compiler.AssertExpectations(t)

			jobSpecRepoFac.On("New", projSpec).Return(jobSpecRepo)
			jobRepo.On("GetAll").Return(jobs, nil)
			// resolve dependencies
			depenResolver.On("Resolve", jobSpecsBase).Return(jobSpecsAfterDepenResolve, nil)
			// resolve priority
			priorityResolver.On("Resolve", jobSpecsAfterDepenResolve).Return(jobSpecsAfterPriorityResolve, nil)
			jobRepoFac.On("New", projSpec).Return(jobRepo, nil)
			// compile to dag and save the first one
			compiler.On("Compile", jobSpecsAfterPriorityResolve[0], projSpec).Return(jobs[0], nil)
			jobRepo.On("Save", jobs[0]).Return(nil)
			// fetch currently stored
			jobSpecRepo.On("GetAll").Return(jobSpecsBase, nil)
			// delete unwanted
			jobSpecRepo.On("Delete", jobs[1].Name).Return(nil)
			jobRepo.On("Delete", jobs[1].Name).Return(nil)

			svc := job.NewService(jobSpecRepoFac, jobRepoFac, compiler, depenResolver, priorityResolver)
			err := svc.Sync(projSpec, nil)
			assert.Nil(t, err)
		})
	})
}
