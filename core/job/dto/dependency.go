package dto

import "github.com/odpf/optimus/core/tenant"

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

type UnresolvedDependency struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (u UnresolvedDependency) IsStaticDependency() bool {
	return u.JobName != ""
}
