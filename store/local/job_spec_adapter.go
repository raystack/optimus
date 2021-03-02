package local

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const (
	JobConfigVersion = 1
)

func init() {
	validator.SetValidationFunc("isCron", utils.CronIntervalValidator)
}

// Job are inputs from user to create a job
// yaml representation of the job
type Job struct {
	Version      int    `yaml:"version,omitempty" validate:"min=1,max=100"`
	Name         string `validate:"min=3,max=1024"`
	Owner        string `yaml:"owner" validate:"min=3,max=1024"`
	Schedule     JobSchedule
	Behavior     JobBehavior
	Task         JobTask
	Asset        map[string]string `yaml:"asset,omitempty"`
	Dependencies []string
	Hooks        []JobHook
}

type JobSchedule struct {
	StartDate string `yaml:"start_date" json:"start_date" validate:"regexp=^\\d{4}-\\d{2}-\\d{2}$"`
	EndDate   string `yaml:"end_date,omitempty" json:"end_date"`
	Interval  string `yaml:"interval" validate:"isCron"`
}

type JobBehavior struct {
	DependsOnPast bool `yaml:"depends_on_past" json:"depends_on_past"`
	Catchup       bool `yaml:"catch_up" json:"catch_up"`
}

type JobTask struct {
	Name   string
	Config map[string]string `yaml:"config,omitempty"`
	Window JobTaskWindow
}

type JobTaskWindow struct {
	Size       string
	Offset     string
	TruncateTo string `yaml:"truncate_to" validate:"regexp=^(h|d|w|)$"`
}

type JobHook struct {
	Name   string
	Config map[string]string `yaml:"config,omitempty"`
}

// ToSpec converts the local's JobHook representation to the optimus' models.JobSpecHook
func (a JobHook) ToSpec(supportedHookRepo models.SupportedHookRepo) (models.JobSpecHook, error) {
	hookUnit, err := supportedHookRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, errors.Wrap(err, "spec reading error")
	}
	return models.JobSpecHook{
		Config: a.Config,
		Unit:   hookUnit,
	}, nil
}

// FromSpec converts the optimus' models.JobSpecHook representation to the local's JobHook
func (a JobHook) FromSpec(spec models.JobSpecHook) JobHook {
	return JobHook{
		Name:   spec.Unit.GetName(),
		Config: spec.Config,
	}
}

func (conf *Job) prepareWindow() (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = time.Hour * 24
	window.Offset = 0
	window.TruncateTo = "d"

	if conf.Task.Window.TruncateTo != "" {
		window.TruncateTo = conf.Task.Window.TruncateTo
	}
	if conf.Task.Window.Size != "" {
		window.Size, err = time.ParseDuration(conf.Task.Window.Size)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window of %s with size %v", conf.Name, conf.Task.Window.Size)
		}
	}
	if conf.Task.Window.Offset != "" {
		window.Offset, err = time.ParseDuration(conf.Task.Window.Offset)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window %s with offset %v", conf.Name, conf.Task.Window.Offset)
		}
	}
	return window, nil
}

type Adapter struct {
	supportedTaskRepo models.SupportedTaskRepo
	supportedHookRepo models.SupportedHookRepo
}

func NewAdapter(supportedTaskRepo models.SupportedTaskRepo, supportedHookRepo models.SupportedHookRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo: supportedTaskRepo,
		supportedHookRepo: supportedHookRepo,
	}
}

func (adapt Adapter) ToSpec(conf Job) (models.JobSpec, error) {
	var err error

	// parse dates
	startDate, err := time.Parse(models.JobDatetimeLayout, conf.Schedule.StartDate)
	if err != nil {
		return models.JobSpec{}, err
	}
	var endDate *time.Time = nil
	if conf.Schedule.EndDate != "" {
		end, err := time.Parse(models.JobDatetimeLayout, conf.Schedule.EndDate)
		if err != nil {
			return models.JobSpec{}, err
		}
		endDate = &end
	}

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	for _, dep := range conf.Dependencies {
		dependencies[dep] = models.JobSpecDependency{
			Type: models.JobSpecDependencyTypeIntra,
		}
	}

	// prep hooks
	var hooks []models.JobSpecHook
	for _, hook := range conf.Hooks {
		adaptHook, err := hook.ToSpec(adapt.supportedHookRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
		hooks = append(hooks, adaptHook)
	}

	// prep window
	window, err := conf.prepareWindow()
	if err != nil {
		return models.JobSpec{}, err
	}

	execUnit, err := adapt.supportedTaskRepo.GetByName(conf.Task.Name)
	if err != nil {
		return models.JobSpec{}, errors.Wrapf(err, "spec reading error, failed to find exec unit %s", conf.Task.Name)
	}

	job := models.JobSpec{
		Version: conf.Version,
		Name:    strings.TrimSpace(conf.Name),
		Owner:   conf.Owner,
		Schedule: models.JobSpecSchedule{
			StartDate: startDate,
			EndDate:   endDate,
			Interval:  conf.Schedule.Interval,
		},
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: false,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: conf.Task.Config,
			Window: window,
		},
		Assets:       models.JobAssets{}.FromMap(conf.Asset),
		Dependencies: dependencies,
		Hooks:        hooks,
	}
	return job, nil
}

func (adapt Adapter) FromSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("exec unit is nil")
	}
	parsed := Job{
		Version: spec.Version,
		Name:    spec.Name,
		Owner:   spec.Owner,
		Schedule: JobSchedule{
			Interval:  spec.Schedule.Interval,
			StartDate: spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		},
		Behavior: JobBehavior{
			DependsOnPast: spec.Behavior.DependsOnPast,
			Catchup:       spec.Behavior.CatchUp,
		},
		Task: JobTask{
			Name:   spec.Task.Unit.GetName(),
			Config: spec.Task.Config,
			Window: JobTaskWindow{
				Size:       spec.Task.Window.SizeString(),
				Offset:     spec.Task.Window.OffsetString(),
				TruncateTo: spec.Task.Window.TruncateTo,
			},
		},
		Asset:        spec.Assets.ToMap(),
		Dependencies: []string{},
		Hooks:        []JobHook{},
	}

	if spec.Schedule.EndDate != nil {
		parsed.Schedule.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name := range spec.Dependencies {
		parsed.Dependencies = append(parsed.Dependencies, name)
	}

	// prep hooks
	for _, hook := range spec.Hooks {
		parsed.Hooks = append(parsed.Hooks, JobHook{}.FromSpec(hook))
	}

	return parsed, nil
}
