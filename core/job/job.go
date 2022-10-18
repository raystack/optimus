package job

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/internal/errors"
)

const EntityJob = "job"

type Job struct {
	jobSpec     *dto.JobSpec
	destination Destination
	sources     []Source
}

func (j Job) JobSpec() *dto.JobSpec {
	return j.jobSpec
}

func (j Job) Destination() Destination {
	return j.destination
}

func (j Job) Sources() []Source {
	return j.sources
}

func NewJob(jobSpec *dto.JobSpec, destination Destination, sources []Source) *Job {
	return &Job{jobSpec: jobSpec, destination: destination, sources: sources}
}

type Destination string

func DestinationFrom(urn string) (Destination, error) {
	if urn == "" {
		return "", errors.InvalidArgument(EntityJob, "destination urn is empty")
	}
	return Destination(urn), nil
}

func (d Destination) String() string {
	return string(d)
}

type Source string

func sourceFrom(urn string) (Source, error) {
	if urn == "" {
		return "", errors.InvalidArgument(EntityJob, "source urn is empty")
	}
	return Source(urn), nil
}

func SourcesFrom(urns []string) ([]Source, error) {
	sources := make([]Source, len(urns))
	for i, urn := range urns {
		source, err := sourceFrom(urn)
		if err != nil {
			return nil, err
		}
		sources[i] = source
	}
	return sources, nil
}

func (s Source) String() string {
	return string(s)
}

type JobName string

func JobNameFrom(urn string) (JobName, error) {
	if urn == "" {
		return "", errors.InvalidArgument(EntityJob, "job name is empty")
	}
	return JobName(urn), nil
}

func (j JobName) String() string {
	return string(j)
}
