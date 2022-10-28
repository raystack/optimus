package job

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
)

const EntityJob = "job"

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
	name                   Name
	projectName            tenant.ProjectName
	dependencies           []*dto.Dependency
	unresolvedDependencies []*dto.UnresolvedDependency
}

func NewWithDependency(name Name, projectName tenant.ProjectName, dependencies []*dto.Dependency, unresolvedDependencies []*dto.UnresolvedDependency) *WithDependency {
	return &WithDependency{name: name, projectName: projectName, dependencies: dependencies, unresolvedDependencies: unresolvedDependencies}
}

func (w WithDependency) Name() Name {
	return w.name
}

func (w WithDependency) ProjectName() tenant.ProjectName {
	return w.projectName
}

func (w WithDependency) Dependencies() []*dto.Dependency {
	return w.dependencies
}

func (w WithDependency) UnresolvedDependencies() []*dto.UnresolvedDependency {
	return w.unresolvedDependencies
}

type JobsWithDependency []*WithDependency

func (j JobsWithDependency) ToJobDependencyMap() map[Name][]*dto.Dependency {
	jobDependencyMap := make(map[Name][]*dto.Dependency)
	for _, jobWithDependency := range j {
		jobDependencyMap[jobWithDependency.name] = jobWithDependency.dependencies
	}
	return jobDependencyMap
}
