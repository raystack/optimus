package job

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const EntityJob = "job"

type Job struct {
	jobSpec     *dto.JobSpec
	destination string
	sources     []string
}

func (j Job) JobSpec() *dto.JobSpec {
	return j.jobSpec
}

func (j Job) Destination() string {
	return j.destination
}

func (j Job) Sources() []string {
	return j.sources
}

func (j Job) StaticDependencyNames() []string {
	return j.jobSpec.Dependencies().JobDependencies()
}

func (j Job) ProjectName() tenant.ProjectName {
	return j.jobSpec.Tenant().Project().Name()
}

func NewJob(jobSpec *dto.JobSpec, destination string, sources []string) *Job {
	return &Job{jobSpec: jobSpec, destination: destination, sources: sources}
}

type Name string

func NameFrom(urn string) (Name, error) {
	if urn == "" {
		return "", errors.InvalidArgument(EntityJob, "job name is empty")
	}
	return Name(urn), nil
}

func (j Name) String() string {
	return string(j)
}

type WithDependency struct {
	name                   Name
	dependencies           []*Dependency
	unresolvedDependencies []*dto.UnresolvedDependency
}

func (w WithDependency) Name() Name {
	return w.name
}

func (w WithDependency) Dependencies() []*Dependency {
	return w.dependencies
}

func (w WithDependency) UnresolvedDependencies() []*dto.UnresolvedDependency {
	return w.unresolvedDependencies
}

func NewWithDependency(name Name, dependencies []*Dependency, unresolvedDependencies []*dto.UnresolvedDependency) *WithDependency {
	return &WithDependency{name: name, dependencies: dependencies, unresolvedDependencies: unresolvedDependencies}
}

type JobsWithDependency []*WithDependency

func (j JobsWithDependency) ToJobDependencyMap() map[Name][]*Dependency {
	jobDependencyMap := make(map[Name][]*Dependency)
	for _, jobWithDependency := range j {
		jobDependencyMap[jobWithDependency.name] = jobWithDependency.dependencies
	}
	return jobDependencyMap
}

type Dependency struct {
	name string

	tnnt tenant.Tenant

	host string

	resource string
}

func (d Dependency) Name() string {
	return d.name
}

func (d Dependency) Tnnt() tenant.Tenant {
	return d.tnnt
}

func (d Dependency) Host() string {
	return d.host
}

func (d Dependency) Resource() string {
	return d.resource
}

func NewDependency(name string, tenant tenant.Tenant, host string, resource string) *Dependency {
	return &Dependency{name: name, tnnt: tenant, host: host, resource: resource}
}

type Dependencies []*Dependency

func (d Dependencies) ToDependencyFullNameMap() map[string]bool {
	fullNameDependencyMap := make(map[string]bool)
	for _, dependency := range d {
		fullName := dependency.tnnt.ProjectName().String() + "/" + dependency.name
		fullNameDependencyMap[fullName] = true
	}
	return fullNameDependencyMap
}

func (d Dependencies) ToDependencyDestinationMap() map[string]bool {
	dependencyDestinationMap := make(map[string]bool)
	for _, dependency := range d {
		dependencyDestinationMap[dependency.resource] = true
	}
	return dependencyDestinationMap
}
