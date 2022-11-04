package resolver

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
)

type DependencyResolver struct {
	jobRepository              JobRepository
	externalDependencyResolver ExternalDependencyResolver
}

func NewDependencyResolver(jobRepository JobRepository, externalDependencyResolver ExternalDependencyResolver) *DependencyResolver {
	return &DependencyResolver{jobRepository: jobRepository, externalDependencyResolver: externalDependencyResolver}
}

type ExternalDependencyResolver interface {
	FetchExternalDependencies(ctx context.Context, unresolvedDependencies []*dto.RawDependency) ([]*job.Dependency, []*dto.RawDependency, error)
}

type JobRepository interface {
	Add(ctx context.Context, jobs []*job.Job) (savedJobs []*job.Job, jobErrors error, err error)
	GetJobNameWithInternalDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Dependency, error)
}

func (d DependencyResolver) Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) (jobsWithAllDependencies []*job.WithDependency, dependencyErrors error, err error) {
	// get internal inferred and static dependencies
	jobNames := job.Jobs(jobs).GetJobNames()
	jobsWithInternalDependencies, err := d.jobRepository.GetJobNameWithInternalDependencies(ctx, projectName, jobNames)
	if err != nil {
		return nil, nil, err
	}

	// merge with external dependencies
	jobsWithAllDependencies, getDependencyErr := d.getJobsWithAllDependencies(ctx, jobs, jobsWithInternalDependencies)
	if getDependencyErr != nil {
		dependencyErrors = multierror.Append(dependencyErrors, getDependencyErr)
	}
	if unresolvedDependencyErrors := d.getUnresolvedDependencyErrors(jobsWithAllDependencies); unresolvedDependencyErrors != nil {
		dependencyErrors = multierror.Append(dependencyErrors, unresolvedDependencyErrors)
	}
	return jobsWithAllDependencies, dependencyErrors, nil
}

func (d DependencyResolver) getJobsWithAllDependencies(ctx context.Context, jobs []*job.Job, jobsWithInternalDependencies map[job.Name][]*job.Dependency) ([]*job.WithDependency, error) {
	var jobsWithAllDependencies []*job.WithDependency
	var allErrors error

	for _, jobEntity := range jobs {
		var allDependencies []*job.Dependency

		// get internal dependencies
		internalDependencies := jobsWithInternalDependencies[jobEntity.Spec().Name()]
		allDependencies = append(allDependencies, internalDependencies...)

		// try to resolve dependencies from external
		unresolvedDependencies := d.identifyUnresolvedDependencies(internalDependencies, jobEntity)
		externalDependencies, unresolvedDependencies, err := d.externalDependencyResolver.FetchExternalDependencies(ctx, unresolvedDependencies)
		if err != nil {
			allErrors = multierror.Append(allErrors, err)
		}
		allDependencies = append(allDependencies, externalDependencies...)

		// include unresolved dependencies
		for _, dep := range unresolvedDependencies {
			allDependencies = append(allDependencies, job.NewDependencyUnresolved(dep.JobName, dep.ResourceURN, dep.ProjectName))
		}

		jobWithAllDependencies := job.NewWithDependency(jobEntity, allDependencies)
		jobsWithAllDependencies = append(jobsWithAllDependencies, jobWithAllDependencies)
	}
	return jobsWithAllDependencies, allErrors
}

func (d DependencyResolver) identifyUnresolvedDependencies(resolvedDependencies []*job.Dependency, jobEntity *job.Job) (unresolvedDependencies []*dto.RawDependency) {
	unresolvedStaticDependencies := d.identifyUnresolvedStaticDependency(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedStaticDependencies...)

	unresolvedInferredDependencies := d.identifyUnresolvedInferredDependencies(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedInferredDependencies...)

	return unresolvedDependencies
}

func (d DependencyResolver) identifyUnresolvedInferredDependencies(resolvedDependencies []*job.Dependency, jobEntity *job.Job) []*dto.RawDependency {
	var unresolvedInferredDependencies []*dto.RawDependency
	resolvedDependencyDestinationMap := job.Dependencies(resolvedDependencies).ToDependencyDestinationMap()
	for _, source := range jobEntity.Sources() {
		if !resolvedDependencyDestinationMap[source] {
			unresolvedInferredDependencies = append(unresolvedInferredDependencies, &dto.RawDependency{
				ResourceURN: source,
			})
		}
	}
	return unresolvedInferredDependencies
}

func (d DependencyResolver) identifyUnresolvedStaticDependency(resolvedDependencies []*job.Dependency, jobEntity *job.Job) []*dto.RawDependency {
	var unresolvedStaticDependencies []*dto.RawDependency
	resolvedDependencyFullNameMap := job.Dependencies(resolvedDependencies).ToDependencyFullNameMap()
	for _, dependencyName := range jobEntity.StaticDependencyNames() {
		var projectDependencyName, jobDependencyName string

		if strings.Contains(dependencyName, "/") {
			projectDependencyName = strings.Split(dependencyName, "/")[0]
			jobDependencyName = strings.Split(dependencyName, "/")[1]
		} else {
			projectDependencyName = jobEntity.ProjectName().String()
			jobDependencyName = dependencyName
		}

		fullDependencyName := jobEntity.ProjectName().String() + "/" + dependencyName
		if !resolvedDependencyFullNameMap[fullDependencyName] {
			unresolvedStaticDependencies = append(unresolvedStaticDependencies, &dto.RawDependency{
				ProjectName: projectDependencyName,
				JobName:     jobDependencyName,
			})
		}
	}
	return unresolvedStaticDependencies
}

func (DependencyResolver) getUnresolvedDependencyErrors(jobsWithDependencies []*job.WithDependency) error {
	var dependencyErr error
	for _, jobWithDependencies := range jobsWithDependencies {
		for _, unresolvedDependency := range jobWithDependencies.GetUnresolvedDependencies() {
			if unresolvedDependency.DependencyType() == job.DependencyTypeStatic {
				errMsg := fmt.Sprintf("[%s] error: %s unknown dependency", jobWithDependencies.Name().String(), unresolvedDependency.Name())
				dependencyErr = multierror.Append(dependencyErr, errors.NewError(errors.ErrNotFound, job.EntityJob, errMsg))
			}
		}
	}
	return dependencyErr
}
