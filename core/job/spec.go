package job

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

const DateLayout = "2006-01-02"

type Spec struct {
	version  Version
	name     Name
	owner    Owner
	schedule *Schedule
	window   models.Window
	task     *Task

	description string
	labels      map[string]string
	metadata    *Metadata
	hooks       []*Hook
	asset       *Asset

	//TODO: rename to AlertSpec
	alerts []*Alert

	//TODO: rename to UpstreamSpec
	upstream *SpecUpstream
}

func (s Spec) Version() Version {
	return s.version
}

func (s Spec) Name() Name {
	return s.name
}

func (s Spec) Owner() Owner {
	return s.owner
}

func (s Spec) Schedule() *Schedule {
	return s.schedule
}

func (s Spec) Window() models.Window {
	return s.window
}

func (s Spec) Task() *Task {
	return s.task
}

func (s Spec) Description() string {
	return s.description
}

func (s Spec) Labels() map[string]string {
	return s.labels
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

func (s Spec) Asset() *Asset {
	return s.asset
}

func (s Spec) Metadata() *Metadata {
	return s.metadata
}

func (s Spec) IsEqual(otherSpec *Spec) bool {
	if s.version != otherSpec.version {
		return false
	}
	if s.name != otherSpec.name {
		return false
	}
	if s.owner != otherSpec.owner {
		return false
	}
	if !reflect.DeepEqual(s.schedule, otherSpec.schedule) {
		return false
	}
	if !reflect.DeepEqual(s.window, otherSpec.window) {
		return false
	}
	if !reflect.DeepEqual(s.task, otherSpec.task) {
		return false
	}
	if s.description != otherSpec.description {
		return false
	}
	if !reflect.DeepEqual(s.labels, otherSpec.labels) {
		return false
	}
	if !reflect.DeepEqual(s.metadata, otherSpec.metadata) {
		return false
	}
	if !reflect.DeepEqual(s.hooks, otherSpec.hooks) {
		return false
	}

	if !reflect.DeepEqual(s.asset, otherSpec.asset) {
		if s.asset == nil && otherSpec.asset.assets != nil {
			return false
		}
		if otherSpec.asset == nil && s.asset.assets != nil {
			return false
		}
		if !reflect.DeepEqual(s.asset.assets, otherSpec.asset.assets) {
			return false
		}
	}

	if !reflect.DeepEqual(s.upstream, otherSpec.upstream) {
		if s.upstream == nil && (otherSpec.upstream.UpstreamNames() != nil || otherSpec.upstream.HTTPUpstreams() != nil) {
			return false
		}
		if otherSpec.upstream == nil && (s.upstream.UpstreamNames() != nil || s.upstream.HTTPUpstreams() != nil) {
			return false
		}
		if s.upstream != nil && otherSpec.upstream != nil {
			if !reflect.DeepEqual(s.upstream.httpUpstreams, otherSpec.upstream.httpUpstreams) || !reflect.DeepEqual(s.upstream.upstreamNames, otherSpec.upstream.upstreamNames) {
				return false
			}
		}
	}

	return reflect.DeepEqual(s.alerts, otherSpec.alerts)
}

type SpecBuilder struct {
	spec *Spec
}

func NewSpecBuilder(
	version Version,
	name Name,
	owner Owner,
	schedule *Schedule,
	window models.Window,
	task *Task,
) *SpecBuilder {
	return &SpecBuilder{
		spec: &Spec{
			version:  version,
			name:     name,
			owner:    owner,
			schedule: schedule,
			window:   window,
			task:     task,
		},
	}
}

func (s *SpecBuilder) Build() *Spec {
	return s.spec
}

func (s *SpecBuilder) WithHooks(hooks []*Hook) *SpecBuilder {
	spec := *s.spec
	spec.hooks = hooks
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithAlerts(alerts []*Alert) *SpecBuilder {
	spec := *s.spec
	spec.alerts = alerts
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithSpecUpstream(specUpstream *SpecUpstream) *SpecBuilder {
	spec := *s.spec
	spec.upstream = specUpstream
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithAsset(asset *Asset) *SpecBuilder {
	spec := *s.spec
	spec.asset = asset
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithMetadata(metadata *Metadata) *SpecBuilder {
	spec := *s.spec
	spec.metadata = metadata
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithLabels(labels map[string]string) *SpecBuilder {
	spec := *s.spec
	spec.labels = labels
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithDescription(description string) *SpecBuilder {
	spec := *s.spec
	spec.description = description
	return &SpecBuilder{
		spec: &spec,
	}
}

type Specs []*Spec

func (s Specs) ToNameAndSpecMap() map[Name]*Spec {
	nameAndSpecMap := make(map[Name]*Spec, len(s))
	for _, spec := range s {
		nameAndSpecMap[spec.Name()] = spec
	}
	return nameAndSpecMap
}

type Version int

func VersionFrom(version int) (Version, error) {
	if version <= 0 {
		return 0, errors.InvalidArgument(EntityJob, "version is less than or equal to zero")
	}
	return Version(version), nil
}

func (v Version) Int() int {
	return int(v)
}

type Name string

func NameFrom(name string) (Name, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJob, "name is empty")
	}
	return Name(name), nil
}

func (n Name) String() string {
	return string(n)
}

type Owner string

func OwnerFrom(owner string) (Owner, error) {
	if owner == "" {
		return "", errors.InvalidArgument(EntityJob, "owner is empty")
	}
	return Owner(owner), nil
}

func (o Owner) String() string {
	return string(o)
}

type ScheduleDate string

func ScheduleDateFrom(date string) (ScheduleDate, error) {
	if date == "" {
		return ScheduleDate(""), nil
	}
	if _, err := time.Parse(DateLayout, date); err != nil {
		msg := fmt.Sprintf("error is encountered when validating date with layout [%s]: %s", DateLayout, err)
		return "", errors.InvalidArgument(EntityJob, msg)
	}
	return ScheduleDate(date), nil
}

func (s ScheduleDate) String() string {
	return string(s)
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

type Schedule struct {
	startDate     ScheduleDate
	endDate       ScheduleDate
	interval      string
	dependsOnPast bool
	catchUp       bool
	retry         *Retry
}

func (s Schedule) StartDate() ScheduleDate {
	return s.startDate
}

func (s Schedule) EndDate() ScheduleDate {
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

type ScheduleBuilder struct {
	schedule *Schedule
}

// TODO: move interval to optional
func NewScheduleBuilder(startDate ScheduleDate) *ScheduleBuilder {
	return &ScheduleBuilder{
		schedule: &Schedule{
			startDate: startDate,
		},
	}
}

func (s ScheduleBuilder) Build() (*Schedule, error) {
	if s.schedule.startDate == "" {
		return nil, errors.InvalidArgument(EntityJob, "start date is empty")
	}
	return s.schedule, nil
}

func (s ScheduleBuilder) WithInterval(interval string) *ScheduleBuilder {
	schedule := *s.schedule
	schedule.interval = interval
	return &ScheduleBuilder{
		schedule: &schedule,
	}
}

func (s ScheduleBuilder) WithEndDate(endDate ScheduleDate) *ScheduleBuilder {
	schedule := *s.schedule
	schedule.endDate = endDate
	return &ScheduleBuilder{
		schedule: &schedule,
	}
}

func (s ScheduleBuilder) WithDependsOnPast(dependsOnPast bool) *ScheduleBuilder {
	schedule := *s.schedule
	schedule.dependsOnPast = dependsOnPast
	return &ScheduleBuilder{
		schedule: &schedule,
	}
}

func (s ScheduleBuilder) WithCatchUp(catchUp bool) *ScheduleBuilder {
	schedule := *s.schedule
	schedule.catchUp = catchUp
	return &ScheduleBuilder{
		schedule: &schedule,
	}
}

func (s ScheduleBuilder) WithRetry(retry *Retry) *ScheduleBuilder {
	schedule := *s.schedule
	schedule.retry = retry
	return &ScheduleBuilder{
		schedule: &schedule,
	}
}

type Config struct {
	configs map[string]string
}

func NewConfig(configs map[string]string) (*Config, error) {
	if err := validateMap(configs); err != nil {
		return nil, err
	}
	return &Config{configs: configs}, nil
}

func (c Config) Configs() map[string]string {
	return c.configs
}

type TaskName string

func TaskNameFrom(name string) (TaskName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJob, "task name is empty")
	}
	return TaskName(name), nil
}

func (t TaskName) String() string {
	return string(t)
}

type Task struct {
	info   *models.PluginInfoResponse
	name   TaskName
	config *Config
}

func (t Task) Name() TaskName {
	return t.name
}

func (t Task) Config() *Config {
	return t.config
}

func (t Task) Info() *models.PluginInfoResponse {
	return t.info
}

type TaskBuilder struct {
	task *Task
}

func NewTaskBuilder(name TaskName, config *Config) *TaskBuilder {
	return &TaskBuilder{
		task: &Task{name: name, config: config},
	}
}

func (t TaskBuilder) WithInfo(info *models.PluginInfoResponse) *TaskBuilder {
	task := *t.task
	task.info = info
	return &TaskBuilder{
		task: &task,
	}
}

func (t TaskBuilder) Build() *Task {
	return t.task
}

type MetadataResourceConfig struct {
	cpu    string
	memory string
}

func (m MetadataResourceConfig) CPU() string {
	return m.cpu
}

func (m MetadataResourceConfig) Memory() string {
	return m.memory
}

func NewMetadataResourceConfig(cpu string, memory string) *MetadataResourceConfig {
	return &MetadataResourceConfig{cpu: cpu, memory: memory}
}

type MetadataResource struct {
	request *MetadataResourceConfig
	limit   *MetadataResourceConfig
}

func (m MetadataResource) Request() *MetadataResourceConfig {
	return m.request
}

func (m MetadataResource) Limit() *MetadataResourceConfig {
	return m.limit
}

func NewResourceMetadata(request *MetadataResourceConfig, limit *MetadataResourceConfig) *MetadataResource {
	return &MetadataResource{request: request, limit: limit}
}

type Metadata struct {
	resource  *MetadataResource
	scheduler map[string]string
}

func (m Metadata) Resource() *MetadataResource {
	return m.resource
}

func (m Metadata) Scheduler() map[string]string {
	return m.scheduler
}

func (m Metadata) validate() error {
	return validateMap(m.scheduler)
}

type MetadataBuilder struct {
	metadata *Metadata
}

func NewMetadataBuilder() *MetadataBuilder {
	return &MetadataBuilder{
		metadata: &Metadata{},
	}
}

func (m MetadataBuilder) Build() (*Metadata, error) {
	if err := m.metadata.validate(); err != nil {
		return nil, err
	}
	return m.metadata, nil
}

func (m MetadataBuilder) WithResource(resource *MetadataResource) *MetadataBuilder {
	metadata := *m.metadata
	metadata.resource = resource
	return &MetadataBuilder{
		metadata: &metadata,
	}
}

func (m MetadataBuilder) WithScheduler(scheduler map[string]string) *MetadataBuilder {
	metadata := *m.metadata
	metadata.scheduler = scheduler
	return &MetadataBuilder{
		metadata: &metadata,
	}
}

type HookName string

func HookNameFrom(name string) (HookName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJob, "name is empty")
	}
	return HookName(name), nil
}

func (h HookName) String() string {
	return string(h)
}

type Hook struct {
	name   HookName
	config *Config
}

func NewHook(name HookName, config *Config) *Hook {
	return &Hook{name: name, config: config}
}

func (h Hook) Name() HookName {
	return h.name
}

func (h Hook) Config() *Config {
	return h.config
}

type Asset struct {
	assets map[string]string
}

func NewAsset(fileNameToContent map[string]string) (*Asset, error) {
	asset := &Asset{assets: fileNameToContent}
	if err := asset.validate(); err != nil {
		return nil, err
	}
	return asset, nil
}

func (a Asset) validate() error {
	return validateMap(a.assets)
}

func (a Asset) Assets() map[string]string {
	return a.assets
}

type EventType string

// TODO: Check which event type that is valid. There should be a validation and also added in the documentation.
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

type Alert struct {
	on       EventType
	channels []string
	config   *Config
}

func (a Alert) On() EventType {
	return a.on
}

func (a Alert) Channels() []string {
	return a.channels
}

func (a Alert) Config() *Config {
	return a.config
}

func (a Alert) validate() error {
	if a.config != nil {
		if err := validateMap(a.config.configs); err != nil {
			return err
		}
	}
	return nil
}

type AlertBuilder struct {
	alert *Alert
}

func NewAlertBuilder(on EventType, channels []string) *AlertBuilder {
	return &AlertBuilder{
		alert: &Alert{
			on:       on,
			channels: channels,
		},
	}
}

func (a AlertBuilder) Build() (*Alert, error) {
	if err := a.alert.validate(); err != nil {
		return nil, err
	}
	return a.alert, nil
}

func (a AlertBuilder) WithConfig(config *Config) *AlertBuilder {
	alert := *a.alert
	alert.config = config
	return &AlertBuilder{
		alert: &alert,
	}
}

// TODO: reconsider whether we still need it or not
type SpecHTTPUpstream struct {
	name    Name
	url     string
	headers map[string]string
	params  map[string]string
}

func (s SpecHTTPUpstream) Name() Name {
	return s.name
}

func (s SpecHTTPUpstream) URL() string {
	return s.url
}

func (s SpecHTTPUpstream) Headers() map[string]string {
	return s.headers
}

func (s SpecHTTPUpstream) Params() map[string]string {
	return s.params
}

func (s SpecHTTPUpstream) validate() error {
	me := errors.NewMultiError("errors on spec http upstream")
	me.Append(validateMap(s.headers))
	me.Append(validateMap(s.params))
	return errors.MultiToError(me)
}

type SpecHTTPUpstreamBuilder struct {
	upstream *SpecHTTPUpstream
}

func NewSpecHTTPUpstreamBuilder(name Name, url string) *SpecHTTPUpstreamBuilder {
	return &SpecHTTPUpstreamBuilder{
		upstream: &SpecHTTPUpstream{
			name: name,
			url:  url,
		},
	}
}

func (s SpecHTTPUpstreamBuilder) Build() (*SpecHTTPUpstream, error) {
	if err := s.upstream.validate(); err != nil {
		return nil, err
	}
	return s.upstream, nil
}

func (s SpecHTTPUpstreamBuilder) WithHeaders(headers map[string]string) *SpecHTTPUpstreamBuilder {
	upstream := *s.upstream
	upstream.headers = headers
	return &SpecHTTPUpstreamBuilder{
		upstream: &upstream,
	}
}

func (s SpecHTTPUpstreamBuilder) WithParams(params map[string]string) *SpecHTTPUpstreamBuilder {
	upstream := *s.upstream
	upstream.params = params
	return &SpecHTTPUpstreamBuilder{
		upstream: &upstream,
	}
}

type SpecUpstreamName string

func (s SpecUpstreamName) String() string {
	return string(s)
}

func SpecUpstreamNameFrom(specUpstreamName string) SpecUpstreamName {
	return SpecUpstreamName(specUpstreamName)
}

func (s SpecUpstreamName) IsWithProjectName() bool {
	return strings.Contains(s.String(), "/")
}

func (s SpecUpstreamName) GetProjectName() (tenant.ProjectName, error) {
	if s.IsWithProjectName() {
		projectNameStr := strings.Split(s.String(), "/")[0]
		return tenant.ProjectNameFrom(projectNameStr)
	}
	return "", errors.NewError(errors.ErrInternalError, EntityJob, "project name in job upstream specification not found")
}

func (s SpecUpstreamName) GetJobName() (Name, error) {
	if s.IsWithProjectName() {
		projectNameStr := strings.Split(s.String(), "/")[1]
		return NameFrom(projectNameStr)
	}
	return NameFrom(s.String())
}

type SpecUpstream struct {
	upstreamNames []SpecUpstreamName
	httpUpstreams []*SpecHTTPUpstream
}

func (s SpecUpstream) UpstreamNames() []SpecUpstreamName {
	return s.upstreamNames
}

func (s SpecUpstream) HTTPUpstreams() []*SpecHTTPUpstream {
	return s.httpUpstreams
}

func (s SpecUpstream) validate() error {
	me := errors.NewMultiError("errors on spec upstream")
	for _, u := range s.httpUpstreams {
		me.Append(u.validate())
	}
	return errors.MultiToError(me)
}

type SpecUpstreamBuilder struct {
	upstream *SpecUpstream
}

func NewSpecUpstreamBuilder() *SpecUpstreamBuilder {
	return &SpecUpstreamBuilder{
		upstream: &SpecUpstream{},
	}
}

func (s SpecUpstreamBuilder) Build() (*SpecUpstream, error) {
	if err := s.upstream.validate(); err != nil {
		return nil, err
	}
	return s.upstream, nil
}

func (s SpecUpstreamBuilder) WithUpstreamNames(names []SpecUpstreamName) *SpecUpstreamBuilder {
	upstream := *s.upstream
	upstream.upstreamNames = names
	return &SpecUpstreamBuilder{
		upstream: &upstream,
	}
}

func (s SpecUpstreamBuilder) WithSpecHTTPUpstream(httpUpstreams []*SpecHTTPUpstream) *SpecUpstreamBuilder {
	upstream := *s.upstream
	upstream.httpUpstreams = httpUpstreams
	return &SpecUpstreamBuilder{
		upstream: &upstream,
	}
}

func NewLabels(labels map[string]string) (map[string]string, error) {
	if err := validateMap(labels); err != nil {
		return nil, err
	}
	return labels, nil
}

// TODO: check whether this is supposed to be here or in utils
func validateMap(input map[string]string) error {
	for key := range input {
		if key == "" {
			return errors.InvalidArgument(EntityJob, "map contains empty key")
		}
	}
	return nil
}
