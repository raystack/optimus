package job

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/internal/errors"
	"golang.org/x/net/context"
	"gorm.io/gorm"
)

type JobDependencyRepository struct {
	db *gorm.DB
}

type JobDependency struct {
	JobName string

	DependencyJobName       string
	DependencyProjectName   string
	DependencyNamespaceName string
	DependencyHost          string
}

func (j JobDependencyRepository) Save(ctx context.Context, jobsWithDependencies []*job.WithDependency) error {
	var storageJobDependencies []*JobDependency
	for _, jobWithDependencies := range jobsWithDependencies {
		dependencies, err := toJobDependency(jobWithDependencies)
		if err != nil {
			return err
		}
		storageJobDependencies = append(storageJobDependencies, dependencies...)
	}

	insertJobDependencyQuery := `
INSERT INTO job_dependency (
	job_name, dependency_job_name,
	dependency_project_name, dependency_namespace_name, dependency_host,
	created_at, updated_at
)
VALUES
	?, ?,
	?, ?, ?,
	NOW(), NOW()
`

	for _, dependency := range storageJobDependencies {
		result := j.db.WithContext(ctx).Exec(insertJobDependencyQuery, dependency.JobName, dependency.DependencyJobName,
			dependency.DependencyProjectName, dependency.DependencyNamespaceName, dependency.DependencyHost)

		if result.Error != nil {
			return errors.Wrap(job.EntityJob, "unable to save job dependency", result.Error)
		}

		if result.RowsAffected == 0 {
			return errors.InternalError(job.EntityJob, "unable to save job dependency, rows affected 0", nil)
		}
	}

	return nil
}

func toJobDependency(jobWithDependency *job.WithDependency) ([]*JobDependency, error) {
	var jobDependencies []*JobDependency
	for _, dependency := range jobWithDependency.Dependencies() {
		namespaceName, err := dependency.Tnnt().NamespaceName()
		if err != nil {
			return nil, err
		}

		jobDependencies = append(jobDependencies, &JobDependency{
			JobName:                 jobWithDependency.Name().String(),
			DependencyJobName:       dependency.Name(),
			DependencyProjectName:   dependency.Tnnt().ProjectName().String(),
			DependencyNamespaceName: namespaceName.String(),
			DependencyHost:          dependency.Host(),
		})
	}
	return jobDependencies, nil
}

/*
	storing the job dependency -> job_dependency: project, namespace, job name, optimus host, type (static/inferred/external)
	ignore dependencies?
*/
