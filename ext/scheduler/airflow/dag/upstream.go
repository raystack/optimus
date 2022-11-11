package dag

import (
	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
)

type Upstreams struct {
	HTTP      []*job_run.HTTPUpstreams
	Upstreams []Upstream
}

func (u Upstreams) Empty() bool {
	if len(u.HTTP) == 0 && len(u.Upstreams) == 0 {
		return true
	}
	return false
}

type Upstream struct {
	JobName  string
	Tenant   tenant.Tenant
	Host     string
	TaskName string
}

func SetupUpstreams(upstreams job_run.Upstreams) Upstreams {
	var ups []Upstream
	for _, u := range upstreams.Upstreams {
		if u.State != "resolved" {
			continue
		}
		upstream := Upstream{
			JobName:  u.JobName,
			Tenant:   u.Tenant,
			Host:     u.Host,
			TaskName: u.TaskName,
		}
		ups = append(ups, upstream)
	}
	return Upstreams{
		HTTP:      upstreams.HTTP,
		Upstreams: ups,
	}
}
