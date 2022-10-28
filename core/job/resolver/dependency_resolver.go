package resolver

import (
	"strings"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/job/service"
)

type DependencyResolver struct {
	jobRepository              service.JobRepository
	externalDependencyResolver ExternalDependencyResolver
}

func NewDependencyResolver(jobRepository service.JobRepository, externalDependencyResolver ExternalDependencyResolver) *DependencyResolver {
	return &DependencyResolver{jobRepository: jobRepository, externalDependencyResolver: externalDependencyResolver}
}

func (d DependencyResolver) Resolve(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error) {
	var jobsWithAllDependencies []*job.WithDependency

	// get internal inferred and static dependencies
	projectName := jobs[0].ProjectName()
	jobNames := job.Jobs(jobs).GetJobNames()
	jobsWithInternalDependencies, err := d.jobRepository.GetJobWithDependencies(ctx, projectName, jobNames)
	if err != nil {
		return nil, err
	}

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
		jobWithAllDependencies := job.NewWithDependency(jobEntity.JobSpec().Name(), jobEntity.ProjectName(), resolvedDependencies, unknownDependencies)
		jobsWithAllDependencies = append(jobsWithAllDependencies, jobWithAllDependencies)
	}

	return jobsWithAllDependencies, nil
}

func (d DependencyResolver) identifyUnresolvedDependencies(resolvedDependencies []*dto.Dependency, jobEntity *job.Job) (unresolvedDependencies []*dto.UnresolvedDependency) {
	unresolvedStaticDependencies := d.identifyUnresolvedStaticDependency(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedStaticDependencies...)

	unresolvedInferredDependencies := d.identifyUnresolvedInferredDependencies(resolvedDependencies, jobEntity)
	unresolvedDependencies = append(unresolvedDependencies, unresolvedInferredDependencies...)

	return unresolvedDependencies
}

func (d DependencyResolver) identifyUnresolvedInferredDependencies(resolvedDependencies []*dto.Dependency, jobEntity *job.Job) []*dto.UnresolvedDependency {
	var unresolvedInferredDependencies []*dto.UnresolvedDependency
	resolvedDependencyDestinationMap := dto.Dependencies(resolvedDependencies).ToDependencyDestinationMap()
	for _, source := range jobEntity.Sources() {
		if !resolvedDependencyDestinationMap[source] {
			unresolvedInferredDependencies = append(unresolvedInferredDependencies, &dto.UnresolvedDependency{
				ResourceURN: source,
			})
		}
	}
	return unresolvedInferredDependencies
}

func (d DependencyResolver) identifyUnresolvedStaticDependency(resolvedDependencies []*dto.Dependency, jobEntity *job.Job) []*dto.UnresolvedDependency {
	var unresolvedStaticDependencies []*dto.UnresolvedDependency
	resolvedDependencyFullNameMap := dto.Dependencies(resolvedDependencies).ToDependencyFullNameMap()
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
