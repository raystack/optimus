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
	// TODO: based on discussion, remove tenant since it is not required in spec
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

type Config struct {
	config map[string]string
}

func (c Config) Config() map[string]string {
	return c.config
}

func NewConfig(config map[string]string) *Config {
	return &Config{config: config}
}

type Task struct {
	name   string
	config *Config
}

func (t Task) Name() string {
	return t.name
}

func (t Task) Config() *Config {
	return t.config
}

func NewTask(name string, config *Config) *Task {
	return &Task{name: name, config: config}
}

type Hook struct {
	name   string
	config *Config
}

func (h Hook) Name() string {
	return h.name
}

func (h Hook) Config() *Config {
	return h.config
}

func NewHook(name string, config *Config) *Hook {
	return &Hook{name: name, config: config}
}

type SpecUpstream struct {
	upstreamNames []string
	httpUpstreams []*HTTPUpstreams
}

func NewSpecUpstream(upstreamNames []string, httpUpstreams []*HTTPUpstreams) *SpecUpstream {
	return &SpecUpstream{upstreamNames: upstreamNames, httpUpstreams: httpUpstreams}
}

func (s SpecUpstream) UpstreamNames() []string {
	return s.upstreamNames
}

func (s SpecUpstream) HTTPUpstreams() []*HTTPUpstreams {
	return s.httpUpstreams
}

type HTTPUpstreams struct {
	name    string
	url     string
	headers map[string]string
	params  map[string]string
}

func (h HTTPUpstreams) Name() string {
	return h.name
}

func (h HTTPUpstreams) URL() string {
	return h.url
}

func (h HTTPUpstreams) Headers() map[string]string {
	return h.headers
}

func (h HTTPUpstreams) Params() map[string]string {
	return h.params
}

func NewHTTPUpstream(name string, url string, headers map[string]string, params map[string]string) *HTTPUpstreams {
	return &HTTPUpstreams{name: name, url: url, headers: headers, params: params}
}

type Schedule struct {
	startDate     string // to check
	endDate       string // to check
	interval      string // to check
	dependsOnPast bool
	catchUp       bool
	retry         *Retry
}

func (s Schedule) StartDate() string {
	return s.startDate
}

func (s Schedule) EndDate() string {
	return s.endDate
}

func (s Schedule) Interval() string {
	return s.interval
}

func (s Schedule) DependsOnPast() bool {
	return s.dependsOnPast
}

func (s Schedule) CatchUp() bool {
	return s.catchUp
}

func (s Schedule) Retry() *Retry {
	return s.retry
}

func NewSchedule(startDate string, endDate string, interval string, dependsOnPast bool, catchUp bool, retry *Retry) *Schedule {
	return &Schedule{startDate: startDate, endDate: endDate, interval: interval, dependsOnPast: dependsOnPast, catchUp: catchUp, retry: retry}
}

type Retry struct {
	count              int
	delay              int32
	exponentialBackoff bool
}

func (r Retry) Count() int {
	return r.count
}

func (r Retry) Delay() int32 {
	return r.delay
}

func (r Retry) ExponentialBackoff() bool {
	return r.exponentialBackoff
}

func NewRetry(count int, delay int32, exponentialBackoff bool) *Retry {
	return &Retry{count: count, delay: delay, exponentialBackoff: exponentialBackoff}
}

type Alert struct {
	on       EventType
	channels []string
	config   map[string]string
}

func (a Alert) On() EventType {
	return a.on
}

func (a Alert) Channels() []string {
	return a.channels
}

func (a Alert) Config() map[string]string {
	return a.config
}

func NewAlert(on EventType, channels []string, config map[string]string) *Alert {
	return &Alert{on: on, channels: channels, config: config}
}

type EventType string

const (
	SLAMissEvent EventType = "sla_miss"

	JobFailureEvent EventType = "failure"
	JobStartEvent   EventType = "job_start"
	JobFailEvent    EventType = "job_fail"
	JobSuccessEvent EventType = "job_success"
	JobRetryEvent   EventType = "retry"

	TaskStartEvent   EventType = "task_start"
	TaskRetryEvent   EventType = "task_retry"
	TaskFailEvent    EventType = "task_fail"
	TaskSuccessEvent EventType = "task_success"

	HookStartEvent   EventType = "hook_start"
	HookRetryEvent   EventType = "hook_retry"
	HookFailEvent    EventType = "hook_fail"
	HookSuccessEvent EventType = "hook_success"

	SensorStartEvent   EventType = "sensor_start"
	SensorRetryEvent   EventType = "sensor_retry"
	SensorFailEvent    EventType = "sensor_fail"
	SensorSuccessEvent EventType = "sensor_success"
)

type Metadata struct {
	resource  *ResourceMetadata
	scheduler map[string]string
}

func (m Metadata) Resource() *ResourceMetadata {
	return m.resource
}

func (m Metadata) Scheduler() map[string]string {
	return m.scheduler
}

func NewMetadata(resource *ResourceMetadata, scheduler map[string]string) *Metadata {
	return &Metadata{resource: resource, scheduler: scheduler}
}

type ResourceMetadata struct {
	request *ResourceConfig
	limit   *ResourceConfig
}

func (r ResourceMetadata) Request() *ResourceConfig {
	return r.request
}

func (r ResourceMetadata) Limit() *ResourceConfig {
	return r.limit
}

func NewResourceMetadata(request *ResourceConfig, limit *ResourceConfig) *ResourceMetadata {
	return &ResourceMetadata{request: request, limit: limit}
}

type ResourceConfig struct {
	cpu    string
	memory string
}

func (r ResourceConfig) CPU() string {
	return r.cpu
}

func (r ResourceConfig) Memory() string {
	return r.memory
}

func NewResourceConfig(cpu string, memory string) *ResourceConfig {
	return &ResourceConfig{cpu: cpu, memory: memory}
}
