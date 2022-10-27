package job_run

import "github.com/odpf/optimus/internal/errors"

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
}

type JobWithDetails struct {
}
