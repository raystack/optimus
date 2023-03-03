package dag

import (
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type Upstreams struct {
	HTTP      []*scheduler.HTTPUpstreams
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

func SetupUpstreams(upstreams scheduler.Upstreams, host string) Upstreams {
	var ups []Upstream
	for _, u := range upstreams.UpstreamJobs {
		var upstreamHost string
		if !u.External {
			upstreamHost = host
		} else {
			upstreamHost = u.Host
		}
		upstream := Upstream{
			JobName:  u.JobName,
			Tenant:   u.Tenant,
			Host:     upstreamHost,
			TaskName: u.TaskName,
		}
		ups = append(ups, upstream)
	}
	return Upstreams{
		HTTP:      upstreams.HTTP,
		Upstreams: ups,
	}
}
