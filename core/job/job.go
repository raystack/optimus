package job

import (
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityJob = "job"

	UpstreamTypeStatic   UpstreamType = "static"
	UpstreamTypeInferred UpstreamType = "inferred"

	UpstreamStateResolved   UpstreamState = "resolved"
	UpstreamStateUnresolved UpstreamState = "unresolved"
)

type Job struct {
	tenant tenant.Tenant

	spec *Spec

	destination ResourceURN
	sources     []ResourceURN
}

func (j Job) Tenant() tenant.Tenant {
	return j.tenant
}

func (j Job) Spec() *Spec {
	return j.spec
}

type ResourceURN string

func ResourceURNFrom(resourceURN string) (ResourceURN, error) {
	if resourceURN == "" {
		return "", errors.InvalidArgument(EntityJob, "resource urn is empty")
	}
	return ResourceURN(resourceURN), nil
}

func (n ResourceURN) String() string {
	return string(n)
}

func (j Job) Destination() ResourceURN {
	return j.destination
}

func (j Job) Sources() []ResourceURN {
	return j.sources
}

func (j Job) StaticUpstreamNames() []SpecUpstreamName {
	if j.spec.upstream == nil {
		return nil
	}
	return j.spec.upstream.UpstreamNames()
}

func (j Job) ProjectName() tenant.ProjectName {
	return j.Tenant().ProjectName()
}

func NewJob(tenant tenant.Tenant, spec *Spec, destination ResourceURN, sources []ResourceURN) *Job {
	return &Job{tenant: tenant, spec: spec, destination: destination, sources: sources}
}

type Jobs []*Job

func (j Jobs) GetJobNames() []Name {
	jobNames := make([]Name, len(j))
	for i, job := range j {
		jobNames[i] = job.spec.Name()
	}
	return jobNames
}

func (j Jobs) GetNameAndSpecMap() map[Name]*Spec {
	nameAndSpecMap := make(map[Name]*Spec, len(j))
	for _, job := range j {
		nameAndSpecMap[job.spec.Name()] = job.spec
	}
	return nameAndSpecMap
}

type WithUpstream struct {
	job       *Job
	upstreams []*Upstream
}

func NewWithUpstream(job *Job, upstreams []*Upstream) *WithUpstream {
	return &WithUpstream{job: job, upstreams: upstreams}
}

func (w WithUpstream) Job() *Job {
	return w.job
}

func (w WithUpstream) Upstreams() []*Upstream {
	return w.upstreams
}

func (w WithUpstream) Name() Name {
	return w.job.spec.Name()
}

func (w WithUpstream) GetUnresolvedUpstreams() []*Upstream {
	var unresolvedUpstreams []*Upstream
	for _, upstream := range w.upstreams {
		if upstream.state == UpstreamStateUnresolved {
			unresolvedUpstreams = append(unresolvedUpstreams, upstream)
		}
	}
	return unresolvedUpstreams
}

type Upstream struct {
	name     Name
	host     string
	resource ResourceURN
	tenant   tenant.Tenant
	_type    UpstreamType
	state    UpstreamState
}

func NewUpstreamResolved(name Name, host string, resource ResourceURN, tnnt tenant.Tenant, typeStr string) (*Upstream, error) {
	upstreamType, err := upstreamTypeFrom(typeStr)
	if err != nil {
		return nil, err
	}

	return &Upstream{name: name, host: host, resource: resource, tenant: tnnt,
		_type: upstreamType, state: UpstreamStateResolved}, nil
}

func NewUpstreamUnresolved(name Name, resource ResourceURN, projectName string) *Upstream {
	var _type UpstreamType
	if name != "" {
		_type = UpstreamTypeStatic
	} else {
		_type = UpstreamTypeInferred
	}

	var tnnt tenant.Tenant
	if projectName != "" {
		tnnt, _ = tenant.NewTenant(projectName, "")
	}

	return &Upstream{name: name, resource: resource, tenant: tnnt, _type: _type,
		state: UpstreamStateUnresolved}
}

func (u Upstream) Name() Name {
	return u.name
}

func (u Upstream) Tenant() tenant.Tenant {
	return u.tenant
}

func (u Upstream) Host() string {
	return u.host
}

func (u Upstream) Resource() ResourceURN {
	return u.resource
}

func (u Upstream) Type() UpstreamType {
	return u._type
}

func (u Upstream) State() UpstreamState {
	return u.state
}

type UpstreamType string

func (d UpstreamType) String() string {
	return string(d)
}

func upstreamTypeFrom(str string) (UpstreamType, error) {
	switch str {
	case UpstreamTypeStatic.String():
		return UpstreamTypeStatic, nil
	case UpstreamTypeInferred.String():
		return UpstreamTypeInferred, nil
	default:
		return "", errors.InvalidArgument(EntityJob, "unknown type for upstream: "+str)
	}
}

type UpstreamState string

func (d UpstreamState) String() string {
	return string(d)
}

func UpstreamStateFrom(str string) (UpstreamState, error) {
	switch str {
	case UpstreamStateResolved.String():
		return UpstreamStateResolved, nil
	case UpstreamStateUnresolved.String():
		return UpstreamStateUnresolved, nil
	default:
		return "", errors.InvalidArgument(EntityJob, "unknown state for upstream: "+str)
	}
}

type Upstreams []*Upstream

func (u Upstreams) ToUpstreamFullNameMap() map[string]bool {
	fullNameUpstreamMap := make(map[string]bool)
	for _, upstream := range u {
		fullName := upstream.tenant.ProjectName().String() + "/" + upstream.name.String()
		fullNameUpstreamMap[fullName] = true
	}
	return fullNameUpstreamMap
}

func (u Upstreams) ToUpstreamDestinationMap() map[ResourceURN]bool {
	upstreamDestinationMap := make(map[ResourceURN]bool)
	for _, upstream := range u {
		upstreamDestinationMap[upstream.resource] = true
	}
	return upstreamDestinationMap
}

type FullName string

func FullNameFrom(projectName tenant.ProjectName, jobName Name) FullName {
	return FullName(projectName.String() + "/" + jobName.String())
}
