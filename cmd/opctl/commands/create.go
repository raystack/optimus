package commands

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"strconv"

	cli "github.com/spf13/cobra"
	"gopkg.in/AlecAivazis/survey.v1"
	"github.com/odpf/optimus/models"
)

const (
	DefaultStencilUrl = "http://odpf/artifactory/proto-descriptors/ocean-proton/latest"
)

var (
	supportedTaskInputs = map[string][]*survey.Question{}
)

//initialize registered tasks
func registerTaskInput() {
	supportedTaskInputs["bq2bq"] = []*survey.Question{
		{
			Name:     "Project",
			Prompt:   &survey.Input{Message: "Project ID:"},
			Validate: validateName,
		},
		{
			Name:     "Dataset",
			Prompt:   &survey.Input{Message: "Dataset Name:"},
			Validate: validateName,
		},
		{
			Name:     "Table",
			Prompt:   &survey.Input{Message: "Table Name:"},
			Validate: validateTableName,
		},
		{
			Name: "LoadMethod",
			Prompt: &survey.Select{
				Message: "Load method to use on destination?",
				Options: []string{"REPLACE", "APPEND", "MERGE"},
				Default: "MERGE",
			},
			Validate: survey.Required,
		},
	}
}

func createCommand(l logger, jobSrv models.JobService) *cli.Command {
	cmd := &cli.Command{
		Use:   "create",
		Short: "Create a new resource",
	}
	cmd.AddCommand(createJobSubCommand(l, jobSrv))
	return cmd
}

func createJobSubCommand(l logger, jobSvc models.JobService) *cli.Command {
	registerTaskInput()
	return &cli.Command{
		Use:   "job",
		Short: "create a new Job",
		RunE: func(cmd *cli.Command, args []string) error {
			jobInput, err := createJobSurvey(l)
			if err != nil {
				return err
			}
			return jobSvc.CreateJob(jobInput)
		},
	}
}

func createJobSurvey(l logger) (models.JobInput, error) {

	availableTasks := []string{}
	for _, task := range models.SupportedTasks.GetAll() {
		availableTasks = append(availableTasks, task.Name)
	}

	var qs = []*survey.Question{
		{
			Name:     "name",
			Prompt:   &survey.Input{Message: "What is the job name?"},
			Validate: validateJobName,
		},
		{
			Name:   "owner",
			Prompt: &survey.Input{Message: "Who is the owner of this job?"},
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
			Name:     "start_date",
			Prompt:   &survey.Input{Message: "Specify the start date (YYYY-MM-DD)"},
			Validate: validateDate,
		},
		{
			Name:     "interval",
			Prompt:   &survey.Input{Message: "Specify the interval (in crontab notation)"},
			Validate: ValidateCronInterval,
		},
	}
	baseInputs := make(map[string]interface{})
	if err := survey.Ask(qs, &baseInputs); err != nil {
		return models.JobInput{}, err
	}

	// define defaults
	jobInput := models.JobInput{
		Version: 1,
		Name:    baseInputs["name"].(string),
		Owner:   baseInputs["owner"].(string),
		Schedule: models.JobInputSchedule{
			StartDate: baseInputs["start_date"].(string),
			Interval:  baseInputs["interval"].(string),
		},
		Task: models.JobInputTask{
			Name:   baseInputs["task"].(string),
			Config: map[string]string{},
			Window: models.JobInputTaskWindow{
				Size:       "24h",
				Offset:     "0",
				TruncateTo: "d",
			},
		},
		Asset: map[string]string{},
		Behavior: models.JobInputBehavior{
			Catchup:       true,
			DependsOnPast: false,
		},
		Dependencies: []string{},
	}

	if questions, ok := supportedTaskInputs[jobInput.Task.Name]; ok {
		taskInputs := make(map[string]interface{})
		if err := survey.Ask(questions, &taskInputs); err != nil {
			return jobInput, err
		}
		taskDetails, err := models.SupportedTasks.GetByName(jobInput.Task.Name)
		if err != nil {
			return jobInput, err
		}

		// process configs
		for key, val := range taskDetails.Config {
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
		for key, val := range taskDetails.Asset {
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

var (
	validateEmail   = validatorFactory.NewFromRegex(`^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$`, "invalid email address")
	validateDate    = validatorFactory.NewFromRegex(`\d{4}-\d{2}-\d{2}`, "date must be in YYYY-MM-DD format")
	validateNoSlash = validatorFactory.NewFromRegex(`^[^/]+$`, "`/` is disallowed")
	validateName    = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_\-]+$`, `invalid name (can only contain characters A-Z (in either case), 0-9, "-" or "_")`),
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

	// taskNames cannot contain slashes, since they're compiled as docker images
	// and using slash may end up causing problems with docker push
	validateJobName = survey.ComposeValidators(validateNoSlash, validateResourceName, survey.MinLength(3), survey.MaxLength(1024))

	// a big query table can only contain the the characters [a-zA-Z0-9_].
	// https://cloud.google.com/bigquery/docs/tables
	validateTableName = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_-]+$`, "invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)"),
		survey.MaxLength(1024),
		survey.MinLength(3),
	)

	validateStencilURL = survey.ComposeValidators(
		func(val interface{}) error {
			str, ok := val.(string)
			if !ok {
				return fmt.Errorf("should be a valid string")
			}
			if regexp.MustCompile(`https?:\/\/([-a-zA-Z0-9@:%._\+~#=]{1,256}\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)
			`).Match([]byte(str)) {
				return fmt.Errorf("should be a valid http/s URL")
			}
			return nil
		},
	)
)
