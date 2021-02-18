package models

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/progress"
)

var (
	ErrNoSuchSpec  = errors.New("job spec not found")
	ErrNoDAGSpecs  = errors.New("no job specifications found")
	ErrNoSuchJob   = errors.New("job not found")
	ErrNoJobs      = errors.New("no job found")
	ErrNoSuchAsset = errors.New("asset not found")
	ErrNoSuchHook  = errors.New("hook not found")
)

const (
	JobDatetimeLayout = "2006-01-02"
)

// JobSpec represents a job
// internal representation of the job
type JobSpec struct {
	ID uuid.UUID

	Version      int
	Name         string
	Owner        string
	Schedule     JobSpecSchedule
	Behavior     JobSpecBehavior
	Task         JobSpecTask
	Dependencies map[string]JobSpecDependency
	Assets       JobAssets
	Hooks        []JobSpecHook
}

func (js *JobSpec) GetHookByName(name string) (JobSpecHook, error) {
	for _, hook := range js.Hooks {
		if hook.Unit.GetName() == name {
			return hook, nil
		}
	}
	return JobSpecHook{}, ErrNoSuchHook
}

type JobSpecSchedule struct {
	StartDate time.Time
	EndDate   *time.Time
	Interval  string
}

type JobSpecBehavior struct {
	DependsOnPast bool
	CatchUp       bool
}

type JobSpecTask struct {
	Unit     Transformation
	Config   map[string]string
	Window   JobSpecTaskWindow
	Priority int
}

type JobSpecTaskWindow struct {
	Size       time.Duration
	Offset     time.Duration
	TruncateTo string
}

func (w *JobSpecTaskWindow) GetStart(scheduledAt time.Time) time.Time {
	s, _ := w.getWindowDate(scheduledAt, w.Size, w.Offset, w.TruncateTo)
	return s
}

func (w *JobSpecTaskWindow) GetEnd(scheduledAt time.Time) time.Time {
	_, e := w.getWindowDate(scheduledAt, w.Size, w.Offset, w.TruncateTo)
	return e
}

func (w *JobSpecTaskWindow) getWindowDate(today time.Time, windowSize, windowOffset time.Duration, windowTruncateTo string) (time.Time, time.Time) {
	floatingEnd := today

	// apply truncation
	if windowTruncateTo == "h" {
		// remove time upto hours
		floatingEnd = floatingEnd.Truncate(time.Hour)
	} else if windowTruncateTo == "d" {
		// remove time upto day
		floatingEnd = floatingEnd.Truncate(24 * time.Hour)
	} else if windowTruncateTo == "w" {
		nearestSunday := time.Duration(time.Saturday-floatingEnd.Weekday()+1) * 24 * time.Hour
		floatingEnd = floatingEnd.Add(nearestSunday)
		floatingEnd = floatingEnd.Truncate(24 * time.Hour)
	}

	// TODO: test if these values are correct
	windowEnd := floatingEnd.Add(windowOffset)
	windowStart := windowEnd.Add(-windowSize)
	return windowStart, windowEnd
}

type JobSpecHook struct {
	Type   string
	Config map[string]string
	Unit   HookUnit
}

type JobSpecAsset struct {
	Name  string
	Value string
}

type JobAssets struct {
	data []JobSpecAsset
}

func (a JobAssets) FromMap(mp map[string]string) JobAssets {
	if len(mp) == 0 {
		return JobAssets{}
	}

	assets := JobAssets{
		data: make([]JobSpecAsset, 0),
	}
	for name, val := range mp {
		assets.data = append(assets.data, JobSpecAsset{
			Name:  name,
			Value: val,
		})
	}
	return assets
}

func (a *JobAssets) ToMap() map[string]string {
	mp := map[string]string{}
	for _, asset := range a.data {
		mp[asset.Name] = asset.Value
	}
	return mp
}

func (a *JobAssets) GetAll() []JobSpecAsset {
	return a.data
}

func (a JobAssets) New(data []JobSpecAsset) *JobAssets {
	return &JobAssets{
		data: data,
	}
}

func (a *JobAssets) GetByName(name string) (JobSpecAsset, error) {
	for _, asset := range a.data {
		if name == asset.Name {
			return asset, nil
		}
	}
	return JobSpecAsset{}, ErrNoSuchAsset
}

func (w *JobSpecTaskWindow) SizeString() string {
	return w.inHrs(int(w.Size.Hours()))
}

func (w *JobSpecTaskWindow) OffsetString() string {
	return w.inHrs(int(w.Offset.Hours()))
}

func (w *JobSpecTaskWindow) inHrs(hrs int) string {
	if hrs == 0 {
		return "0"
	}
	return fmt.Sprintf("%dh", hrs)
}

func (w *JobSpecTaskWindow) String() string {
	return fmt.Sprintf("size_%dh", int(w.Size.Hours()))
}

type JobSpecDependency struct {
	Job *JobSpec
	// TODO specify type of depen, if its static or gen at runtime
	// Type {STATIC, RUNTIME}
}

// JobService provides a high-level operations on DAGs
type JobService interface {
	// Create constructs a Job and commits it to a storage
	Create(JobSpec, ProjectSpec) error
	GetByName(string, ProjectSpec) (JobSpec, error)
	Sync(ProjectSpec, progress.Observer) error
}

// JobCompiler takes template file of a scheduler and after applying
// variables generates a executable input for scheduler.
type JobCompiler interface {
	Compile(JobSpec, ProjectSpec) (Job, error)
}

// Job represents a compiled consumable item for scheduler
// this is generated from JobSpec
type Job struct {
	Name     string
	Contents []byte
}
