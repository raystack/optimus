package job_run

import (
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const EntityJobRun = "jobRun"

type JobName string

func JobNameFrom(name string) (JobName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJobRun, "job name is empty")
	}

	return JobName(name), nil
}

func (n JobName) String() string {
	return string(n)
}

type Job struct {
	JobName JobName
	tenant  tenant.Tenant
}

func (j *Job) Tenant() tenant.Tenant {
	return j.tenant
}

type JobWithDetails struct {
}
