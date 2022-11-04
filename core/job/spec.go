package job

import (
	"fmt"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

const DateSpecLayout = "2006-01-02"

type Spec struct {
	tenant tenant.Tenant

	version     int
	name        Name
	owner       string
	description string
	labels      map[string]string
	schedule    *Schedule
	window      models.Window
	task        *Task
	hooks       []*Hook
	alerts      []*Alert
	upstream    *SpecUpstream
	assets      map[string]string
	metadata    *Metadata
}

func (s Spec) Window() models.Window {
	return s.window
}

func (s Spec) Tenant() tenant.Tenant {
	return s.tenant
}

func (s Spec) Version() int {
	return s.version
}

func (s Spec) Name() Name {
	return s.name
}

func (s Spec) Owner() string {
	return s.owner
}

func (s Spec) Description() string {
	return s.description
}

func (s Spec) Labels() map[string]string {
	return s.labels
}

func (s Spec) Schedule() *Schedule {
	return s.schedule
}

func (s Spec) Task() *Task {
	return s.task
}

func (s Spec) Hooks() []*Hook {
	return s.hooks
}

func (s Spec) Alerts() []*Alert {
	return s.alerts
}

func (s Spec) Upstream() *SpecUpstream {
	return s.upstream
}

func (s Spec) Assets() map[string]string {
	return s.assets
}

func (s Spec) Metadata() *Metadata {
	return s.metadata
}

func (s Spec) Validate() error {
	if s.Schedule().StartDate() == "" {
		return errors.InvalidArgument(EntityJob, "start date cannot be empty")
	}

	if _, err := time.Parse(DateSpecLayout, s.Schedule().StartDate()); err != nil {
		return errors.InvalidArgument(EntityJob, fmt.Sprintf("start date format should be %s", DateSpecLayout))
	}

	if s.Schedule().EndDate() != "" {
		if _, err := time.Parse(DateSpecLayout, s.Schedule().EndDate()); err != nil {
			return errors.InvalidArgument(EntityJob, fmt.Sprintf("end date format should be %s", DateSpecLayout))
		}
	}
	return nil
}

func NewSpec(tenant tenant.Tenant, version int, name string, owner string, description string,
	labels map[string]string, schedule *Schedule, window models.Window, task *Task, hooks []*Hook, alerts []*Alert,
	specUpstreams *SpecUpstream, assets map[string]string, metadata *Metadata) (*Spec, error) {
	jobName, err := NameFrom(name)
	if err != nil {
		return nil, err
	}

	return &Spec{tenant: tenant, version: version, name: jobName, owner: owner, description: description,
		labels: labels, schedule: schedule, window: window, task: task, hooks: hooks, alerts: alerts,
		upstream: specUpstreams, assets: assets, metadata: metadata}, nil
}

type Name string

func NameFrom(urn string) (Name, error) {
	if urn == "" {
		return "", errors.InvalidArgument(EntityJob, "job name is empty")
	}
	return Name(urn), nil
}

func (j Name) String() string {
	return string(j)
}

type Window struct {
	size       string
	offset     string
	truncateTo string
}

func (w Window) Size() string {
	return w.size
}

func (w Window) Offset() string {
	return w.offset
}

func (w Window) TruncateTo() string {
	return w.truncateTo
}
