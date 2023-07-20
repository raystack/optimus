package job

import (
	"fmt"
	"strings"

	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/errors"
)

const (
	EntityJob = "job"

	UpstreamTypeStatic   UpstreamType = "static"
	UpstreamTypeInferred UpstreamType = "inferred"

	UpstreamStateResolved   UpstreamState = "resolved"
	UpstreamStateUnresolved UpstreamState = "unresolved"

	MetricJobEvent                      = "job_events_total"
	MetricJobEventStateAdded            = "added"
	MetricJobEventStateUpdated          = "updated"
	MetricJobEventStateDeleted          = "deleted"
	MetricJobEventStateUpsertFailed     = "upsert_failed"
	MetricJobEventStateDeleteFailed     = "delete_failed"
	MetricJobEventStateValidationFailed = "validation_failed"
	MetricJobEventEnabled               = "enabled"
	MetricJobEventDisabled              = "disabled"

	MetricJobRefreshResourceDownstream = "refresh_resource_downstream_total"
)

type Job struct {
	tenant tenant.Tenant

	spec *Spec

	destination ResourceURN
	sources     []ResourceURN
}

func (j *Job) Tenant() tenant.Tenant {
	return j.tenant
}

func (j *Job) Spec() *Spec {
	return j.spec
}

func (j *Job) GetName() string {
	return j.spec.name.String()
}

func (j *Job) FullName() string {
	return j.ProjectName().String() + "/" + j.spec.name.String()
}

func (j *Job) GetJobWithUnresolvedUpstream() (*WithUpstream, error) {
	unresolvedStaticUpstreams, err := j.getStaticUpstreamsToResolve()
	if err != nil {
		err = errors.InvalidArgument(EntityJob, fmt.Sprintf("failed to get static upstreams to resolve for job %s", j.GetName()))
	}
	unresolvedInferredUpstreams := j.getInferredUpstreamsToResolve()
	allUpstreams := unresolvedStaticUpstreams
	allUpstreams = append(allUpstreams, unresolvedInferredUpstreams...)

	return NewWithUpstream(j, allUpstreams), err
}

func (j *Job) getInferredUpstreamsToResolve() []*Upstream {
	var unresolvedInferredUpstreams []*Upstream
	for _, source := range j.sources {
		unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, NewUpstreamUnresolvedInferred(source))
	}
	return unresolvedInferredUpstreams
}

func (j *Job) getStaticUpstreamsToResolve() ([]*Upstream, error) {
	var unresolvedStaticUpstreams []*Upstream
	me := errors.NewMultiError("get static upstream to resolve errors")

	for _, upstreamName := range j.StaticUpstreamNames() {
		jobUpstreamName, err := upstreamName.GetJobName()
		if err != nil {
			me.Append(err)
			continue
		}

		var projectUpstreamName tenant.ProjectName
		if upstreamName.IsWithProjectName() {
			projectUpstreamName, err = upstreamName.GetProjectName()
			if err != nil {
				me.Append(err)
				continue
			}
		} else {
			projectUpstreamName = j.ProjectName()
		}

		unresolvedStaticUpstreams = append(unresolvedStaticUpstreams, NewUpstreamUnresolvedStatic(jobUpstreamName, projectUpstreamName))
	}
	return unresolvedStaticUpstreams, me.ToErr()
}

type ResourceURN string

func (n ResourceURN) String() string {
	return string(n)
}

func (j *Job) Destination() ResourceURN {
	return j.destination
}

func (j *Job) Sources() []ResourceURN {
	return j.sources
}

func (j *Job) StaticUpstreamNames() []SpecUpstreamName {
	if j.spec.upstreamSpec == nil {
		return nil
	}
	return j.spec.upstreamSpec.UpstreamNames()
}

func (j *Job) ProjectName() tenant.ProjectName {
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

func (j Jobs) GetNameAndJobMap() map[Name]*Job {
	nameAndJobMap := make(map[Name]*Job, len(j))
	for _, job := range j {
		nameAndJobMap[job.spec.Name()] = job
	}
	return nameAndJobMap
}

func (j Jobs) GetNamespaceNameAndJobsMap() map[tenant.NamespaceName][]*Job {
	jobsPerNamespaceName := make(map[tenant.NamespaceName][]*Job, len(j))
	for _, job := range j {
		jobsPerNamespaceName[job.tenant.NamespaceName()] = append(jobsPerNamespaceName[job.tenant.NamespaceName()], job)
	}
	return jobsPerNamespaceName
}

func (j Jobs) GetSpecs() []*Spec {
	var specs []*Spec
	for _, currentJob := range j {
		specs = append(specs, currentJob.spec)
	}
	return specs
}

func (j Jobs) GetJobsWithUnresolvedUpstreams() ([]*WithUpstream, error) {
	me := errors.NewMultiError("get unresolved upstreams errors")

	var jobsWithUnresolvedUpstream []*WithUpstream
	for _, subjectJob := range j {
		jobWithUnresolvedUpstream, err := subjectJob.GetJobWithUnresolvedUpstream()
		me.Append(err)
		jobsWithUnresolvedUpstream = append(jobsWithUnresolvedUpstream, jobWithUnresolvedUpstream)
	}

	return jobsWithUnresolvedUpstream, me.ToErr()
}

type WithUpstream struct {
	job       *Job
	upstreams []*Upstream
}

func NewWithUpstream(job *Job, upstreams []*Upstream) *WithUpstream {
	return &WithUpstream{job: job, upstreams: upstreams}
}

func (w WithUpstream) GetName() string { // to support multiroot DataTree
	return w.job.spec.name.String()
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

func (w WithUpstream) GetResolvedUpstreams() []*Upstream {
	var resolvedUpstreams []*Upstream
	for _, upstream := range w.upstreams {
		if upstream.state == UpstreamStateResolved {
			resolvedUpstreams = append(resolvedUpstreams, upstream)
		}
	}
	return resolvedUpstreams
}

type WithUpstreams []*WithUpstream

func (w WithUpstreams) GetSubjectJobNames() []Name {
	names := make([]Name, len(w))
	for i, withUpstream := range w {
		names[i] = withUpstream.Name()
	}
	return names
}

func (w WithUpstreams) MergeWithResolvedUpstreams(resolvedUpstreamsBySubjectJobMap map[Name][]*Upstream) []*WithUpstream {
	var jobsWithMergedUpstream []*WithUpstream
	for _, jobWithUnresolvedUpstream := range w {
		resolvedUpstreams := resolvedUpstreamsBySubjectJobMap[jobWithUnresolvedUpstream.Name()]
		resolvedUpstreamMapByFullName := Upstreams(resolvedUpstreams).ToFullNameAndUpstreamMap()
		resolvedUpstreamMapByDestination := Upstreams(resolvedUpstreams).ToResourceDestinationAndUpstreamMap()

		var mergedUpstream []*Upstream
		for _, unresolvedUpstream := range jobWithUnresolvedUpstream.Upstreams() {
			if resolvedUpstream, ok := resolvedUpstreamMapByFullName[unresolvedUpstream.FullName()]; ok {
				mergedUpstream = append(mergedUpstream, resolvedUpstream)
				continue
			}
			if resolvedUpstream, ok := resolvedUpstreamMapByDestination[unresolvedUpstream.Resource().String()]; ok {
				mergedUpstream = append(mergedUpstream, resolvedUpstream)
				continue
			}
			mergedUpstream = append(mergedUpstream, unresolvedUpstream)
		}
		distinctMergedUpstream := Upstreams(mergedUpstream).Deduplicate()
		jobsWithMergedUpstream = append(jobsWithMergedUpstream, NewWithUpstream(jobWithUnresolvedUpstream.Job(), distinctMergedUpstream))
	}
	return jobsWithMergedUpstream
}

type Upstream struct {
	name     Name
	host     string
	resource ResourceURN
	taskName TaskName

	projectName   tenant.ProjectName
	namespaceName tenant.NamespaceName

	_type UpstreamType
	state UpstreamState

	external bool
}

func NewUpstreamResolved(name Name, host string, resource ResourceURN, jobTenant tenant.Tenant, upstreamType UpstreamType, taskName TaskName, external bool) *Upstream {
	return &Upstream{
		name:          name,
		host:          host,
		resource:      resource,
		projectName:   jobTenant.ProjectName(),
		namespaceName: jobTenant.NamespaceName(),
		taskName:      taskName,
		_type:         upstreamType,
		state:         UpstreamStateResolved,
		external:      external,
	}
}

func NewUpstreamUnresolvedInferred(resource ResourceURN) *Upstream {
	return &Upstream{resource: resource, _type: UpstreamTypeInferred, state: UpstreamStateUnresolved}
}

func NewUpstreamUnresolvedStatic(name Name, projectName tenant.ProjectName) *Upstream {
	return &Upstream{name: name, projectName: projectName, _type: UpstreamTypeStatic, state: UpstreamStateUnresolved}
}

func (u *Upstream) Name() Name {
	return u.name
}

func (u *Upstream) Host() string {
	return u.host
}

func (u *Upstream) Resource() ResourceURN {
	return u.resource
}

func (u *Upstream) Type() UpstreamType {
	return u._type
}

func (u *Upstream) State() UpstreamState {
	return u.state
}

func (u *Upstream) ProjectName() tenant.ProjectName {
	return u.projectName
}

func (u *Upstream) NamespaceName() tenant.NamespaceName {
	return u.namespaceName
}

func (u *Upstream) External() bool {
	return u.external
}

func (u *Upstream) TaskName() TaskName {
	return u.taskName
}

func (u *Upstream) FullName() string {
	return u.projectName.String() + "/" + u.name.String()
}

type UpstreamType string

func (d UpstreamType) String() string {
	return string(d)
}

func UpstreamTypeFrom(str string) (UpstreamType, error) {
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

type Upstreams []*Upstream

func (u Upstreams) ToFullNameAndUpstreamMap() map[string]*Upstream {
	fullNameUpstreamMap := make(map[string]*Upstream)
	for _, upstream := range u {
		fullName := upstream.ProjectName().String() + "/" + upstream.name.String()
		fullNameUpstreamMap[fullName] = upstream
	}
	return fullNameUpstreamMap
}

func (u Upstreams) ToResourceDestinationAndUpstreamMap() map[string]*Upstream {
	resourceDestinationUpstreamMap := make(map[string]*Upstream)
	for _, upstream := range u {
		if upstream.resource == "" {
			continue
		}
		resourceDestinationUpstreamMap[upstream.resource.String()] = upstream
	}
	return resourceDestinationUpstreamMap
}

func (u Upstreams) Deduplicate() []*Upstream {
	resolvedUpstreamMap := make(map[string]*Upstream)
	unresolvedStaticUpstreamMap := make(map[string]*Upstream)
	unresolvedInferredUpstreamMap := make(map[string]*Upstream)

	for _, upstream := range u {
		if upstream.state == UpstreamStateUnresolved && upstream._type == UpstreamTypeStatic {
			unresolvedStaticUpstreamMap[upstream.FullName()] = upstream
			continue
		}

		if upstream.state == UpstreamStateUnresolved && upstream._type == UpstreamTypeInferred {
			unresolvedInferredUpstreamMap[upstream.resource.String()] = upstream
			continue
		}

		if upstreamInMap, ok := resolvedUpstreamMap[upstream.FullName()]; ok {
			// keep static upstreams in the map if exists
			if upstreamInMap._type == UpstreamTypeStatic {
				continue
			}
		}
		resolvedUpstreamMap[upstream.FullName()] = upstream
	}

	return mapsToUpstreams(resolvedUpstreamMap, unresolvedInferredUpstreamMap, unresolvedStaticUpstreamMap)
}

func mapsToUpstreams(upstreamsMaps ...map[string]*Upstream) []*Upstream {
	var result []*Upstream
	for _, upstreamsMap := range upstreamsMaps {
		for _, upstream := range upstreamsMap {
			result = append(result, upstream)
		}
	}
	return result
}

type FullName string

func FullNameFrom(projectName tenant.ProjectName, jobName Name) FullName {
	return FullName(projectName.String() + "/" + jobName.String())
}

func (f FullName) String() string {
	return string(f)
}

type FullNames []FullName

func (f FullNames) String() string {
	var fullNamesStr []string
	for _, fullName := range f {
		fullNamesStr = append(fullNamesStr, fullName.String())
	}
	return strings.Join(fullNamesStr, ", ")
}

type Downstream struct {
	name Name

	projectName   tenant.ProjectName
	namespaceName tenant.NamespaceName

	taskName TaskName
}

func NewDownstream(name Name, projectName tenant.ProjectName, namespaceName tenant.NamespaceName, taskName TaskName) *Downstream {
	return &Downstream{name: name, projectName: projectName, namespaceName: namespaceName, taskName: taskName}
}

func (d Downstream) Name() Name {
	return d.name
}

func (d Downstream) ProjectName() tenant.ProjectName {
	return d.projectName
}

func (d Downstream) NamespaceName() tenant.NamespaceName {
	return d.namespaceName
}

func (d Downstream) TaskName() TaskName {
	return d.taskName
}

func (d Downstream) FullName() FullName {
	return FullNameFrom(d.projectName, d.name)
}

type DownstreamList []*Downstream

func (d DownstreamList) GetDownstreamFullNames() FullNames {
	var fullNames []FullName
	for _, downstream := range d {
		fullNames = append(fullNames, downstream.FullName())
	}
	return fullNames
}
