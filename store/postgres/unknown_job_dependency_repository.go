package postgres

import (
	"context"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type unknownJobDependencyRepository struct {
	db *gorm.DB
}

func NewUnknownJobDependencyRepository(db *gorm.DB) store.UnknownJobDependencyRepository {
	return &unknownJobDependencyRepository{
		db: db,
	}
}

func (repo unknownJobDependencyRepository) GetUnknownResourceDependencyNamesByJobName(ctx context.Context, projectID models.ProjectID) (map[string][]string, error) {
	type jobNameDependencyPair struct {
		JobName               string
		DependencyResourceURN string
	}
	var jobNameDependencyPairs []jobNameDependencyPair
	resourceDestinationList := repo.db.Select("destination").Table("job")

	if err := repo.db.WithContext(ctx).Select("j.name as job_name, js.resource_urn as dependency_resource_urn").
		Table("job_source js").Joins("join job j on js.job_id = j.id").
		Where("js.resource_urn not in (?) and project_id = ?", resourceDestinationList, projectID.UUID()).Find(&jobNameDependencyPairs).Error; err != nil {
		return nil, err
	}

	dependencyNamesByJobName := make(map[string][]string)
	for _, pair := range jobNameDependencyPairs {
		dependencyNamesByJobName[pair.JobName] = append(dependencyNamesByJobName[pair.JobName], pair.DependencyResourceURN)
	}
	return dependencyNamesByJobName, nil
}

func (repo unknownJobDependencyRepository) GetUnknownStaticDependencyNamesByJobName(ctx context.Context, projectID models.ProjectID) (map[string][]string, error) {
	type jobNameDependencyPair struct {
		JobName           string
		DependencyJobName string
	}
	var jobNameDependencyPairs []jobNameDependencyPair
	jobNameList := repo.db.Select("name").Table("job")

	projectAndJobNameList := repo.db.Select("concat(p.name || '/' || j.name) as dependency_job_name").
		Table("job j").Joins("project p on j.project_id = j.project_id")

	jobNameAndDependencyNameList := repo.db.Select("name, jsonb_object_keys(dependencies) as job_dependency_name").Table("job")

	if err := repo.db.WithContext(ctx).Select("job_name, job_dependency_name").
		Table("(?) j", jobNameAndDependencyNameList).
		Where("job_dependency_name not in (?) and job_dependency_name not in (?) and project_id = ?", jobNameList, projectAndJobNameList, projectID.UUID()).Find(&jobNameDependencyPairs).Error; err != nil {
		return nil, err
	}

	dependencyNamesByJobName := make(map[string][]string)
	for _, pair := range jobNameDependencyPairs {
		dependencyNamesByJobName[pair.JobName] = append(dependencyNamesByJobName[pair.JobName], pair.DependencyJobName)
	}
	return dependencyNamesByJobName, nil
}
