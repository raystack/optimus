package models

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNoSuchSpec  = errors.New("job spec not found")
	ErrNoDAGSpecs  = errors.New("no job specifications found")
	ErrNoSuchJob   = errors.New("job not found")
	ErrNoJobs      = errors.New("no job found")
	ErrNoSuchAsset = errors.New("asset not found")
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

	Hooks []JobSpecHook
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
	Unit   ExecUnit
	Config map[string]string
	Window JobSpecTaskWindow
}

type JobSpecTaskWindow struct {
	Size       time.Duration
	Offset     time.Duration
	TruncateTo string
}

type JobSpecHook struct {
	Name   string
	Config map[string]string
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
	// CreateJob constructs a Job and commits it to a storage
	CreateJob(JobSpec, ProjectSpec) error
	Upload(ProjectSpec) error
}

// JobCompiler TODO...
type JobCompiler interface {
	Compile(JobSpec) (Job, error)
}

// Job represents a compiled consumeable item for scheduler
// this is generated from JobSpec
type Job struct {
	Name     string
	Contents []byte
}
