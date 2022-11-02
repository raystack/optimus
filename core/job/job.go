package job

import (
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityJob = "job"

	DependencyTypeStatic   DependencyType = "static"
	DependencyTypeInferred DependencyType = "inferred"

	DependencyStateResolved   DependencyState = "resolved"
	DependencyStateUnresolved DependencyState = "unresolved"
)

type Job struct {
	jobSpec     *JobSpec
	destination string
	sources     []string
}

func (j Job) JobSpec() *JobSpec {
	return j.jobSpec
}

func (j Job) Destination() string {
	return j.destination
}

func (j Job) Sources() []string {
	return j.sources
}

func (j Job) StaticDependencyNames() []string {
	if j.jobSpec.dependencies == nil {
		return nil
	}
	return j.jobSpec.dependencies.JobDependencies()
}

func (j Job) ProjectName() tenant.ProjectName {
	return j.jobSpec.Tenant().ProjectName()
}

func NewJob(jobSpec *JobSpec, destination string, sources []string) *Job {
	return &Job{jobSpec: jobSpec, destination: destination, sources: sources}
}

type Jobs []*Job

func (j Jobs) GetJobNames() []Name {
	jobNames := make([]Name, len(j))
	for i, job := range j {
		jobNames[i] = job.jobSpec.Name()
	}
	return jobNames
}

type WithDependency struct {
	job          *Job
	dependencies []*Dependency
}

func NewWithDependency(job *Job, dependencies []*Dependency) *WithDependency {
	return &WithDependency{job: job, dependencies: dependencies}
}

func (w WithDependency) Job() *Job {
	return w.job
}

func (w WithDependency) Dependencies() []*Dependency {
	return w.dependencies
}

func (w WithDependency) Name() Name {
	return w.job.jobSpec.Name()
}

func (w WithDependency) GetUnresolvedDependencies() []*Dependency {
	var unresolvedDependencies []*Dependency
	for _, dependency := range w.dependencies {
		if dependency.dependencyState == DependencyStateUnresolved {
			unresolvedDependencies = append(unresolvedDependencies, dependency)
		}
	}
	return unresolvedDependencies
}

type Dependency struct {
	name             string
	host             string
	resource         string
	dependencyTenant tenant.Tenant
	dependencyType   DependencyType
	dependencyState  DependencyState
}

func NewDependencyResolved(name string, host string, resource string, dependencyTenant tenant.Tenant, dependencyTypeStr string) (*Dependency, error) {
	dependencyType, err := dependencyTypeFrom(dependencyTypeStr)
	if err != nil {
		return nil, err
	}

	return &Dependency{name: name, host: host, resource: resource, dependencyTenant: dependencyTenant,
		dependencyType: dependencyType, dependencyState: DependencyStateResolved}, nil
}

func NewDependencyUnresolved(name string, resource string, projectName string) *Dependency {
	var dependencyType DependencyType
	if name != "" {
		dependencyType = DependencyTypeStatic
	} else {
		dependencyType = DependencyTypeInferred
	}

	var dependencyTenant tenant.Tenant
	if projectName != "" {
		dependencyTenant, _ = tenant.NewTenant(projectName, "")
	}

	return &Dependency{name: name, resource: resource, dependencyTenant: dependencyTenant, dependencyType: dependencyType,
		dependencyState: DependencyStateUnresolved}
}

func (d Dependency) Name() string {
	return d.name
}

func (d Dependency) Tenant() tenant.Tenant {
	return d.dependencyTenant
}

func (d Dependency) Host() string {
	return d.host
}

func (d Dependency) Resource() string {
	return d.resource
}

func (d Dependency) DependencyType() DependencyType {
	return d.dependencyType
}

func (d Dependency) DependencyState() DependencyState {
	return d.dependencyState
}

type DependencyType string

func (d DependencyType) String() string {
	return string(d)
}

func dependencyTypeFrom(str string) (DependencyType, error) {
	switch str {
	case DependencyTypeStatic.String():
		return DependencyTypeStatic, nil
	case DependencyTypeInferred.String():
		return DependencyTypeInferred, nil
	default:
		return "", errors.InvalidArgument(EntityJob, "unknown type for dependency: "+str)
	}
}

type DependencyState string

func (d DependencyState) String() string {
	return string(d)
}

func DependencyStateFrom(str string) (DependencyState, error) {
	switch str {
	case DependencyStateResolved.String():
		return DependencyStateResolved, nil
	case DependencyStateUnresolved.String():
		return DependencyStateUnresolved, nil
	default:
		return "", errors.InvalidArgument(EntityJob, "unknown state for dependency: "+str)
	}
}

type Dependencies []*Dependency

func (d Dependencies) ToDependencyFullNameMap() map[string]bool {
	fullNameDependencyMap := make(map[string]bool)
	for _, dependency := range d {
		fullName := dependency.dependencyTenant.ProjectName().String() + "/" + dependency.name
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
