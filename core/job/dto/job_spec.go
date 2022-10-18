package dto

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
)

type JobSpec struct {
	tenant *tenant.WithDetails

	version      int
	name         job.JobName
	owner        string
	description  string
	labels       map[string]string
	schedule     *Schedule
	window       *Window
	task         *Task
	hooks        []*Hook
	alerts       []*Alert
	dependencies *Dependencies
	assets       map[string]string
	metadata     *Metadata
}

func (j JobSpec) Tenant() *tenant.WithDetails {
	return j.tenant
}

func (j JobSpec) Version() int {
	return j.version
}

func (j JobSpec) Name() job.JobName {
	return j.name
}

func (j JobSpec) Owner() string {
	return j.owner
}

func (j JobSpec) Description() string {
	return j.description
}

func (j JobSpec) Labels() map[string]string {
	return j.labels
}

func (j JobSpec) Schedule() *Schedule {
	return j.schedule
}

func (j JobSpec) Window() *Window {
	return j.window
}

func (j JobSpec) Task() *Task {
	return j.task
}

func (j JobSpec) Hooks() []*Hook {
	return j.hooks
}

func (j JobSpec) Alerts() []*Alert {
	return j.alerts
}

func (j JobSpec) Dependencies() *Dependencies {
	return j.dependencies
}

func (j JobSpec) Assets() map[string]string {
	return j.assets
}

func (j JobSpec) Metadata() *Metadata {
	return j.metadata
}

func NewJobSpec(tenant *tenant.WithDetails, version int, name string, owner string, description string,
	labels map[string]string, schedule *Schedule, window *Window, task *Task, hooks []*Hook, alerts []*Alert,
	dependencies *Dependencies, assets map[string]string, metadata *Metadata) (*JobSpec, error) {

	jobName, err := job.JobNameFrom(name)
	if err != nil {
		return nil, err
	}

	return &JobSpec{tenant: tenant, version: version, name: jobName, owner: owner, description: description,
		labels: labels, schedule: schedule, window: window, task: task, hooks: hooks, alerts: alerts,
		dependencies: dependencies, assets: assets, metadata: metadata}, nil
}

/*
behavior:
- validate
- get dstart dend of window
- diff?
*/

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

func NewWindow(size string, offset string, truncateTo string) *Window {
	return &Window{size: size, offset: offset, truncateTo: truncateTo}
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

type Dependencies struct {
	jobDependencies  []string
	httpDependencies []*HttpDependency
}

func (d Dependencies) JobDependencies() []string {
	return d.jobDependencies
}

func (d Dependencies) HttpDependencies() []*HttpDependency {
	return d.httpDependencies
}

func NewDependencies(jobDependencies []string, httpDependencies []*HttpDependency) *Dependencies {
	return &Dependencies{jobDependencies: jobDependencies, httpDependencies: httpDependencies}
}

type HttpDependency struct {
	name    string
	url     string
	headers map[string]string
	params  map[string]string
}

func (h HttpDependency) Name() string {
	return h.name
}

func (h HttpDependency) Url() string {
	return h.url
}

func (h HttpDependency) Headers() map[string]string {
	return h.headers
}

func (h HttpDependency) Params() map[string]string {
	return h.params
}

func NewHttpDependency(name string, url string, headers map[string]string, params map[string]string) *HttpDependency {
	return &HttpDependency{name: name, url: url, headers: headers, params: params}
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

func (r ResourceConfig) Cpu() string {
	return r.cpu
}

func (r ResourceConfig) Memory() string {
	return r.memory
}

func NewResourceConfig(cpu string, memory string) *ResourceConfig {
	return &ResourceConfig{cpu: cpu, memory: memory}
}
