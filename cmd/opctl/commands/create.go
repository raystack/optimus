package commands

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"text/template"
	"time"

	"strconv"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
)

func createCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "create",
		Short: "Create a new resource",
	}
	cmd.AddCommand(createJobSubCommand(l, jobSpecRepo))
	cmd.AddCommand(createHookSubCommand(l, jobSpecRepo))
	return cmd
}

func createJobSubCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {
	return &cli.Command{
		Use:   "job",
		Short: "create a new Job",
		RunE: func(cmd *cli.Command, args []string) error {
			jobInput, err := createJobSurvey(jobSpecRepo)
			if err != nil {
				return err
			}
			spec, err := local.NewAdapter(models.TaskRegistry, models.HookRegistry).ToSpec(jobInput)
			if err != nil {
				return err
			}
			return jobSpecRepo.Save(spec)
		},
	}
}

func createJobSurvey(jobSpecRepo store.JobSpecRepository) (local.Job, error) {
	// TODO: take an additional input "--spec-dir" with default as "." in order to save job specs to a specific directory
	availableTasks := []string{}
	for _, task := range models.TaskRegistry.GetAll() {
		availableTasks = append(availableTasks, task.GetName())
	}

	var qs = []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the job name?",
			},
			Validate: survey.ComposeValidators(validateJobName, IsJobNameUnique(jobSpecRepo)),
		},
		{
			Name: "owner",
			Prompt: &survey.Input{
				Message: "Who is the owner of this job?",
				Help:    "Email or username",
			},
			Validate: survey.Required,
		},
		{
			Name: "task",
			Prompt: &survey.Select{
				Message: "Which task to run?",
				Options: availableTasks,
			},
			Validate: survey.Required,
		},
		{
			Name: "start_date",
			Prompt: &survey.Input{
				Message: "Specify the start date",
				Help:    "Format: (YYYY-MM-DD)",
				Default: time.Now().UTC().Format(models.JobDatetimeLayout),
			},
			Validate: validateDate,
		},
		{
			Name: "interval",
			Prompt: &survey.Input{
				Message: "Specify the interval (in crontab notation)",
				Default: "0 2 * * *",
				Help:    "0 2 * * * / @daily / @hourly",
			},
			Validate: ValidateCronInterval,
		},
	}
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(qs, &baseInputsRaw); err != nil {
		return local.Job{}, err
	}
	baseInputs, err := convertToStringMap(baseInputsRaw)
	if err != nil {
		return local.Job{}, err
	}

	// define defaults
	jobInput := local.Job{
		Version: local.JobConfigVersion,
		Name:    baseInputs["name"],
		Owner:   baseInputs["owner"],
		Schedule: local.JobSchedule{
			StartDate: baseInputs["start_date"],
			Interval:  baseInputs["interval"],
		},
		Task: local.JobTask{
			Name:   baseInputs["task"],
			Config: map[string]string{},
			Window: local.JobTaskWindow{
				Size:       "24h",
				Offset:     "0",
				TruncateTo: "d",
			},
		},
		Asset: map[string]string{},
		Behavior: local.JobBehavior{
			Catchup:       true,
			DependsOnPast: false,
		},
		Dependencies: []local.JobDependency{},
		Hooks:        []local.JobHook{},
	}

	executionTask, err := models.TaskRegistry.GetByName(jobInput.Task.Name)
	if err != nil {
		return jobInput, err
	}

	questions := executionTask.GetQuestions()
	if len(questions) > 0 {
		taskInputsRaw := make(map[string]interface{})
		if err := survey.Ask(questions, &taskInputsRaw); err != nil {
			return jobInput, err
		}

		taskInputs, err := convertToStringMap(taskInputsRaw)
		if err != nil {
			return jobInput, err
		}

		// process configs
		for key, val := range executionTask.GetConfig() {
			tmpl, err := template.New(key).Parse(val)
			if err != nil {
				return jobInput, err
			}
			var buf bytes.Buffer
			if err = tmpl.Execute(&buf, taskInputs); err != nil {
				return jobInput, err
			}
			jobInput.Task.Config[key] = strings.TrimSpace(buf.String())
		}

		// process assets
		for key, val := range executionTask.GetAssets() {
			tmpl, err := template.New(key).Parse(val)
			if err != nil {
				return jobInput, err
			}
			var buf bytes.Buffer
			if err = tmpl.Execute(&buf, taskInputs); err != nil {
				return jobInput, err
			}
			jobInput.Asset[key] = strings.TrimSpace(buf.String())
		}
	}

	return jobInput, nil
}

func createHookSubCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "hook",
		Short: "create a new Hook",
		RunE: func(cmd *cli.Command, args []string) error {
			selectJobName, err := selectJobSurvey(jobSpecRepo)
			if err != nil {
				return err
			}
			jobSpec, err := jobSpecRepo.GetByName(selectJobName)
			if err != nil {
				return err
			}
			jobSpec, err = createHookSurvey(l, jobSpec)
			if err != nil {
				return err
			}
			return jobSpecRepo.Save(jobSpec)
		},
	}
	return cmd
}

func createHookSurvey(l logger, jobSpec models.JobSpec) (models.JobSpec, error) {
	emptyJobSpec := models.JobSpec{}
	var availableHooks []string
	for _, hook := range models.HookRegistry.GetAll() {
		// TODO: this should be generated at runtime based on what base task is
		// selected, support it when we add more than one type of task
		availableHooks = append(availableHooks, hook.GetName())
	}

	var qs = []*survey.Question{
		{
			Name: "hook",
			Prompt: &survey.Select{
				Message: "Which hook to run?",
				Options: availableHooks,
			},
			Validate: survey.Required,
		},
	}
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(qs, &baseInputsRaw); err != nil {
		return emptyJobSpec, err
	}
	baseInputs, err := convertToStringMap(baseInputsRaw)
	if err != nil {
		return emptyJobSpec, err
	}

	selectedHook := baseInputs["hook"]
	if ifHookAlreadyExistsForJob(jobSpec, selectedHook) {
		return emptyJobSpec, errors.Errorf("hook %s already exists for this job", selectedHook)
	}

	executionHook, err := models.HookRegistry.GetByName(selectedHook)
	if err != nil {
		return emptyJobSpec, err
	}

	questions := executionHook.GetQuestions()
	if len(questions) > 0 {
		taskInputsRaw := make(map[string]interface{})
		if err := survey.Ask(questions, &taskInputsRaw); err != nil {
			return emptyJobSpec, err
		}

		taskInputs, err := convertToStringMap(taskInputsRaw)
		if err != nil {
			return emptyJobSpec, err
		}

		hookConfig := map[string]string{}
		rawConfigs, err := executionHook.GetConfig(models.UnitData{
			Config: jobSpec.Task.Config,
			Assets: jobSpec.Assets.ToMap(),
		})
		if err != nil {
			return emptyJobSpec, err
		}
		for key, val := range rawConfigs {
			tmpl, err := template.New(key).Parse(val)
			if err != nil {
				return emptyJobSpec, err
			}
			var buf bytes.Buffer
			if err = tmpl.Execute(&buf, taskInputs); err != nil {
				return emptyJobSpec, err
			}
			hookConfig[key] = strings.TrimSpace(buf.String())
		}
		jobSpec.Hooks = append(jobSpec.Hooks, models.JobSpecHook{
			Unit:   executionHook,
			Config: hookConfig,
		})
	}
	return jobSpec, nil
}

// selectJobSurvey runs a survey to select a job and returns its name
func selectJobSurvey(jobSpecRepo store.JobSpecRepository) (string, error) {
	var allJobNames []string
	jobs, err := jobSpecRepo.GetAll()
	if err != nil {
		return "", err
	}
	for _, job := range jobs {
		allJobNames = append(allJobNames, job.Name)
	}
	selectJob := ""
	if err := survey.AskOne(&survey.Select{
		Message: "Select a Job",
		Options: allJobNames,
	}, &selectJob); err != nil {
		return "", err
	}
	return selectJob, nil
}

func ifHookAlreadyExistsForJob(jobSpec models.JobSpec, newHookName string) bool {
	for _, hook := range jobSpec.Hooks {
		if hook.Unit.GetName() == newHookName {
			return true
		}
	}
	return false
}

func convertToStringMap(inputs map[string]interface{}) (map[string]string, error) {
	conv := map[string]string{}

	for key, val := range inputs {
		switch reflect.TypeOf(val).Name() {
		case "int":
			conv[key] = strconv.Itoa(val.(int))
		case "string":
			conv[key] = val.(string)
		case "OptionAnswer":
			conv[key] = val.(survey.OptionAnswer).Value
		default:
			return conv, errors.New("unknown type found while parsing user inputs")
		}
	}
	return conv, nil
}

var (
	validateEmail   = validatorFactory.NewFromRegex(`^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$`, "invalid email address")
	validateDate    = validatorFactory.NewFromRegex(`\d{4}-\d{2}-\d{2}`, "date must be in YYYY-MM-DD format")
	validateNoSlash = validatorFactory.NewFromRegex(`^[^/]+$`, "`/` is disallowed")
	validateName    = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_\-]+$`, `can only contain characters A-Z (in either case), 0-9, "-" or "_"`),
		survey.MinLength(3),
	)
	validateGreaterThanZero = func(val interface{}) error {
		v, err := strconv.Atoi(val.(string))
		if err != nil {
			return fmt.Errorf("value should be integer")
		}
		if v <= 0 {
			return fmt.Errorf("value needs to be greater than zero")
		}
		return nil
	}

	validateResourceName = validatorFactory.NewFromRegex(`^[a-zA-Z0-9][a-zA-Z0-9_\-\.]+$`, `invalid name (can only contain characters A-Z (in either case), 0-9, "-", "_" or "." and must start with an alphanumeric character)`)

	validateJobName = survey.ComposeValidators(validateNoSlash, validateResourceName, survey.MinLength(3),
		survey.MaxLength(1024))
)

// IsJobNameUnique return a validator that checks if the job already exists with the same name
func IsJobNameUnique(repository store.JobSpecRepository) survey.Validator {
	return func(val interface{}) error {
		if str, ok := val.(string); ok {
			if _, err := repository.GetByName(str); err == nil {
				return fmt.Errorf("job with the provided name already exists")
			}
		} else {
			// otherwise we cannot convert the value into a string and cannot find a job name
			return fmt.Errorf("invalid type of job name %v", reflect.TypeOf(val).Name())
		}
		// the input is fine
		return nil
	}
}
