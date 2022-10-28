package job

import (
	"golang.org/x/net/context"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/internal/errors"
)

type JobDependencyRepository struct {
	db *gorm.DB
}

func NewJobDependencyRepository(db *gorm.DB) *JobDependencyRepository {
	return &JobDependencyRepository{db: db}
}

type JobWithDependency struct {
	JobName                 string
	ProjectName             string
	DependencyJobName       string
	DependencyProjectName   string
	DependencyNamespaceName string
	DependencyResource      string
	DependencyHost          string
}

func (j JobWithDependency) getJobFullName() string {
	return j.ProjectName + "/" + j.JobName
}

func (j JobDependencyRepository) Save(ctx context.Context, jobsWithDependencies []*job.WithDependency) error {
	var storageJobDependencies []*JobWithDependency
	for _, jobWithDependencies := range jobsWithDependencies {
		dependencies, err := toJobDependency(jobWithDependencies)
		if err != nil {
			return err
		}
		storageJobDependencies = append(storageJobDependencies, dependencies...)
	}

	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := deleteDependencies(ctx, tx, storageJobDependencies); err != nil {
			return err
		}

		return insertDependencies(ctx, tx, storageJobDependencies)
	})
}

func insertDependencies(ctx context.Context, tx *gorm.DB, storageJobDependencies []*JobWithDependency) error {
	insertJobDependencyQuery := `
INSERT INTO job_dependency (
	job_name, project_name, dependency_job_name,
	dependency_project_name, dependency_namespace_name, dependency_host,
	created_at, updated_at
)
VALUES (
	?, ?, ?,
	?, ?, ?,
	NOW(), NOW()
);
`

	for _, dependency := range storageJobDependencies {
		result := tx.WithContext(ctx).Exec(insertJobDependencyQuery, dependency.JobName, dependency.ProjectName,
			dependency.DependencyJobName, dependency.DependencyProjectName, dependency.DependencyNamespaceName, dependency.DependencyHost)

		if result.Error != nil {
			return errors.Wrap(job.EntityJob, "unable to save job dependency", result.Error)
		}

		if result.RowsAffected == 0 {
			return errors.InternalError(job.EntityJob, "unable to save job dependency, rows affected 0", nil)
		}
	}
	return nil
}

func deleteDependencies(ctx context.Context, tx *gorm.DB, jobDependencies []*JobWithDependency) error {
	var result *gorm.DB

	var jobFullName []string
	for _, dependency := range jobDependencies {
		jobFullName = append(jobFullName, dependency.getJobFullName())
	}

	deleteForProjectScope := `DELETE
FROM job_dependency
WHERE project_name || '/' || job_name in (?);
`

	result = tx.WithContext(ctx).Exec(deleteForProjectScope, jobFullName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during delete of job dependency", result.Error)
	}

	return nil
}

func toJobDependency(jobWithDependency *job.WithDependency) ([]*JobWithDependency, error) {
	var jobDependencies []*JobWithDependency
	for _, dependency := range jobWithDependency.Dependencies() {
		namespaceName, err := dependency.Tnnt().NamespaceName()
		if err != nil {
			return nil, err
		}

		jobDependencies = append(jobDependencies, &JobWithDependency{
			JobName:                 jobWithDependency.Name().String(),
			ProjectName:             jobWithDependency.ProjectName().String(),
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
