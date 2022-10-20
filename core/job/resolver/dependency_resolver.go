package resolver

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	jobStore "github.com/odpf/optimus/internal/store/postgres/job"
	"golang.org/x/net/context"
	"strings"
)

type DependencyResolver struct {
	jobRepository              jobStore.JobRepository
	externalDependencyResolver ExternalDependencyResolver
}

func (d DependencyResolver) Resolve(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error) {
	var jobsWithAllDependencies []*job.WithDependency

	// with inferred and static dependencies
	jobsWithInternalDependencies, err := d.jobRepository.GetJobWithDependencies(ctx, jobs)
	if err != nil {
		return nil, err
	}

	// check for unresolved dependencies
	resolvedJobDependencyMap := job.JobsWithDependency(jobsWithInternalDependencies).ToJobDependencyMap()
	for _, jobEntity := range jobs {
		// check unresolved dependencies
		resolvedDependencies := resolvedJobDependencyMap[jobEntity.JobSpec().Name()]
		unresolvedDependencies := d.identifyUnresolvedDependencies(resolvedDependencies, jobEntity)

		// try to resolve dependencies from external
		externalDependencies, unknownDependencies, err := d.externalDependencyResolver.FetchExternalDependencies(ctx, unresolvedDependencies)
		if err != nil {
			return nil, err
		}

		// merge all dependencies
		resolvedDependencies = append(resolvedDependencies, externalDependencies...)
		jobWithAllDependencies := job.NewWithDependency(jobEntity.JobSpec().Name(), resolvedDependencies, unknownDependencies)
		jobsWithAllDependencies = append(jobsWithAllDependencies, jobWithAllDependencies)
	}

	return jobsWithAllDependencies, nil
}

func (d DependencyResolver) identifyUnresolvedDependencies(resolvedDependencies []*job.Dependency, jobEntity *job.Job) (unresolvedDependencies []*dto.UnresolvedDependency) {
	unresolvedStaticDependencies := d.identifyUnresolvedStaticDependency(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedStaticDependencies...)

	unresolvedInferredDependencies := d.identifyUnresolvedInferredDependencies(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedInferredDependencies...)

	return unresolvedDependencies
}

func (d DependencyResolver) identifyUnresolvedInferredDependencies(resolvedDependencies []*job.Dependency, jobEntity *job.Job) []*dto.UnresolvedDependency {
	var unresolvedInferredDependencies []*dto.UnresolvedDependency
	resolvedDependencyDestinationMap := job.Dependencies(resolvedDependencies).ToDependencyDestinationMap()
	for _, source := range jobEntity.Sources() {
		if !resolvedDependencyDestinationMap[source] {
			unresolvedInferredDependencies = append(unresolvedInferredDependencies, &dto.UnresolvedDependency{
				ResourceURN: source,
			})
		}
	}
	return unresolvedInferredDependencies
}

func (d DependencyResolver) identifyUnresolvedStaticDependency(resolvedDependencies []*job.Dependency, jobEntity *job.Job) []*dto.UnresolvedDependency {
	var unresolvedStaticDependencies []*dto.UnresolvedDependency
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
			unresolvedStaticDependencies = append(unresolvedStaticDependencies, &dto.UnresolvedDependency{
				ProjectName: projectDependencyName,
				JobName:     jobDependencyName,
			})
		}
	}
	return unresolvedStaticDependencies
}
