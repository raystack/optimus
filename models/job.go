package models

import (
	"errors"
	"time"

	"gopkg.in/validator.v2"
	"github.com/odpf/optimus/utils"
)

func init() {
	validator.SetValidationFunc("isCron", utils.CronIntervalValidator)
}

// JobInput are inputs from user to create a job
// external representation of the job
type JobInput struct {
	Version      int    `yaml:"version,omitempty" validate:"min=1,max=100"`
	Name         string `validate:"min=3,max=1024"`
	Owner        string `yaml:"owner" validate:"min=3,max=1024"`
	Schedule     JobInputSchedule
	Behavior     JobInputBehavior
	Task         JobInputTask
	Asset        map[string]string `yaml:"asset,omitempty"`
	Dependencies []string
}

type JobInputSchedule struct {
	StartDate string `yaml:"start_date" json:"start_date" validate:"regexp=^\\d{4}-\\d{2}-\\d{2}$"`
	EndDate   string `yaml:"end_date,omitempty" json:"end_date"`
	Interval  string `yaml:"interval" validate:"isCron"`
}

type JobInputBehavior struct {
	DependsOnPast bool `yaml:"depends_on_past" json:"depends_on_past"`
	Catchup       bool `yaml:"catch_up" json:"catch_up"`
}

type JobInputTask struct {
	Name   string
	Config map[string]string `yaml:"config,omitempty"`
	Window JobInputTaskWindow
}

type JobInputTaskWindow struct {
	Size       string
	Offset     string
	TruncateTo string `yaml:"truncate_to" validate:"regexp=^(h|d|w|)$"`
}

// JobSpecFactory generates a new JobSpec
// by processing JobInput
type JobSpecFactory interface {
	CreateJobSpec(inputs JobInput) (JobSpec, error)
}

// JobSpec represents a job
// internal representation of the job
type JobSpec struct {
	Version      int
	Name         string
	Owner        string
	Schedule     JobSpecSchedule
	Behavior     JobSpecBehavior
	Task         JobSpecTask
	Dependencies map[string]JobSpecDependency
	Asset        map[string]string
}

type JobSpecSchedule struct {
	StartDate time.Time
	EndDate   *time.Time
	Interval  string
}

type JobSpecBehavior struct {
	DependsOnPast bool
	Catchup       bool
}

type JobSpecTask struct {
	Name   string
	Config map[string]string
	Window TaskWindow
}

type JobSpecDependency struct {
	Name string
	Job  *JobSpec
}

// JobSpecRepository represents a storage interface for Job specifications
type JobSpecRepository interface {
	Save(JobInput) error
	GetByName(string) (JobSpec, error)
	GetAll() ([]JobSpec, error)
}

// JobService provides a high-level operations on DAGs
// This forms the "use case" layer for DAGs
type JobService interface {
	// CreateJob constructs a DAG and commits it to a storage
	CreateJob(inputs JobInput) error
}

// errors returned by JobRepository
var (
	ErrNoSuchSpec = errors.New("dag spec not found")
	ErrNoDAGSpecs = errors.New("no dag specifications found")
	ErrNoSuchDAG  = errors.New("dag not found")
	ErrNoDAGs     = errors.New("no dag found")
)
