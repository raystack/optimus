package job

import (
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
)

const DateLayout = "2006-01-02"

type Spec struct {
	version  int
	name     Name
	owner    string
	schedule *Schedule
	window   models.Window
	task     Task

	description  string
	labels       map[string]string
	metadata     *Metadata
	hooks        []*Hook
	asset        Asset
	alertSpecs   []*AlertSpec
	upstreamSpec *UpstreamSpec
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

func (s Spec) Schedule() *Schedule {
	return s.schedule
}

func (s Spec) Window() models.Window {
	return s.window
}

func (s Spec) Task() Task {
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

func (s Spec) AlertSpecs() []*AlertSpec {
	return s.alertSpecs
}

func (s Spec) UpstreamSpec() *UpstreamSpec {
	return s.upstreamSpec
}

func (s Spec) Asset() Asset {
	return s.asset
}

func (s Spec) Metadata() *Metadata {
	return s.metadata
}

type SpecBuilder struct {
	spec *Spec
}

func NewSpecBuilder(
	version int,
	name Name,
	owner string,
	schedule *Schedule,
	window models.Window,
	task Task,
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

func (s *SpecBuilder) Build() (*Spec, error) {
	if s.spec.version <= 0 {
		return nil, errors.InvalidArgument(EntityJob, "version is less than or equal to zero")
	}
	if s.spec.owner == "" {
		return nil, errors.InvalidArgument(EntityJob, "owner is empty")
	}
	return s.spec, nil
}

func (s *SpecBuilder) WithHooks(hooks []*Hook) *SpecBuilder {
	spec := *s.spec
	spec.hooks = hooks
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithAlerts(alerts []*AlertSpec) *SpecBuilder {
	spec := *s.spec
	spec.alertSpecs = alerts
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithSpecUpstream(specUpstream *UpstreamSpec) *SpecBuilder {
	spec := *s.spec
	spec.upstreamSpec = specUpstream
	return &SpecBuilder{
		spec: &spec,
	}
}

func (s *SpecBuilder) WithAsset(asset Asset) *SpecBuilder {
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

type ScheduleDate string

func ScheduleDateFrom(date string) (ScheduleDate, error) {
	if date == "" {
		return "", nil
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

type Config map[string]string

func ConfigFrom(configs map[string]string) (Config, error) {
	if err := validateMap(configs); err != nil {
		return nil, err
	}
	return configs, nil
}

func (c Config) Map() map[string]string {
	return c
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
	name   TaskName
	config Config
}

func (t Task) Name() TaskName {
	return t.name
}

func (t Task) Config() Config {
	return t.config
}

type TaskBuilder struct {
	task Task
}

func NewTaskBuilder(name TaskName, config Config) *TaskBuilder {
	return &TaskBuilder{
		task: Task{name: name, config: config},
	}
}

func (t TaskBuilder) Build() Task {
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

func NewMetadataResourceConfig(cpu, memory string) *MetadataResourceConfig {
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

func NewResourceMetadata(request, limit *MetadataResourceConfig) *MetadataResource {
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

type Hook struct {
	name   string
	config Config
}

func NewHook(name string, config Config) (*Hook, error) {
	if name == "" {
		return nil, errors.InvalidArgument(EntityJob, "hook name is empty")
	}
	return &Hook{name: name, config: config}, nil
}

func (h Hook) Name() string {
	return h.name
}

func (h Hook) Config() Config {
	return h.config
}

type Asset map[string]string

func AssetFrom(fileNameToContent map[string]string) (Asset, error) {
	asset := Asset(fileNameToContent)
	if err := asset.validate(); err != nil {
		return nil, err
	}
	return asset, nil
}

func (a Asset) Map() map[string]string {
	return a
}

func (a Asset) validate() error {
	return validateMap(a)
}

type AlertSpec struct {
	on string

	channels []string
	config   Config
}

func (a AlertSpec) On() string {
	return a.on
}

func (a AlertSpec) Channels() []string {
	return a.channels
}

func (a AlertSpec) Config() Config {
	return a.config
}

func (a AlertSpec) validate() error {
	if a.config != nil {
		if err := validateMap(a.config); err != nil {
			return err
		}
	}
	return nil
}

type AlertBuilder struct {
	alert *AlertSpec
}

func NewAlertBuilder(on string, channels []string) *AlertBuilder {
	return &AlertBuilder{
		alert: &AlertSpec{
			on:       on,
			channels: channels,
		},
	}
}

func (a AlertBuilder) Build() (*AlertSpec, error) {
	if err := a.alert.validate(); err != nil {
		return nil, err
	}
	return a.alert, nil
}

func (a AlertBuilder) WithConfig(config Config) *AlertBuilder {
	alert := *a.alert
	alert.config = config
	return &AlertBuilder{
		alert: &alert,
	}
}

// TODO: reconsider whether we still need it or not
type SpecHTTPUpstream struct {
	name    string
	url     string
	headers map[string]string
	params  map[string]string
}

func (s SpecHTTPUpstream) Name() string {
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

func NewSpecHTTPUpstreamBuilder(name, url string) *SpecHTTPUpstreamBuilder {
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

type UpstreamSpec struct {
	upstreamNames []SpecUpstreamName
	httpUpstreams []*SpecHTTPUpstream
}

func (s UpstreamSpec) UpstreamNames() []SpecUpstreamName {
	return s.upstreamNames
}

func (s UpstreamSpec) HTTPUpstreams() []*SpecHTTPUpstream {
	return s.httpUpstreams
}

func (s UpstreamSpec) validate() error {
	me := errors.NewMultiError("errors on spec upstream")
	for _, u := range s.httpUpstreams {
		me.Append(u.validate())
	}
	return errors.MultiToError(me)
}

type SpecUpstreamBuilder struct {
	upstream *UpstreamSpec
}

func NewSpecUpstreamBuilder() *SpecUpstreamBuilder {
	return &SpecUpstreamBuilder{
		upstream: &UpstreamSpec{},
	}
}

func (s SpecUpstreamBuilder) Build() (*UpstreamSpec, error) {
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
