package survey

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/AlecAivazis/survey/v2"

	"github.com/raystack/optimus/client/local"
	"github.com/raystack/optimus/client/local/model"
	"github.com/raystack/optimus/internal/models"
	"github.com/raystack/optimus/internal/utils"
	"github.com/raystack/optimus/sdk/plugin"
)

const (
	ISODateLayout         = "2006-01-02"
	jobSpecDefaultVersion = 2
)

var (
	validateDate         = utils.ValidatorFactory.NewFromRegex(`\d{4}-\d{2}-\d{2}`, "date must be in YYYY-MM-DD format")
	validateNoSlash      = utils.ValidatorFactory.NewFromRegex(`^[^/]+$`, "`/` is disallowed")
	validateResourceName = utils.ValidatorFactory.NewFromRegex(`^[a-zA-Z0-9][a-zA-Z0-9_\-\.]+$`,
		`invalid name (can only contain characters A-Z (in either case), 0-9, "-", "_" or "." and must start with an alphanumeric character)`)
	validateJobName = survey.ComposeValidators(validateNoSlash, validateResourceName, survey.MinLength(3),
		survey.MaxLength(220))
)

// JobCreateSurvey defines survey for job creation operation
type JobCreateSurvey struct {
	jobSurvey *JobSurvey
}

// NewJobCreateSurvey initializes job create survey
func NewJobCreateSurvey() *JobCreateSurvey {
	return &JobCreateSurvey{
		jobSurvey: NewJobSurvey(),
	}
}

// AskToCreateJob asks questions to create job
func (j *JobCreateSurvey) AskToCreateJob(pluginRepo *models.PluginRepository, jobSpecReader local.SpecReader[*model.JobSpec], jobDir, defaultJobName string) (model.JobSpec, error) {
	availableTaskNames := j.getAvailableTaskNames(pluginRepo)
	if len(availableTaskNames) == 0 {
		return model.JobSpec{}, errors.New("no supported task plugin found")
	}

	createQuestions := j.getCreateQuestions(jobSpecReader, jobDir, defaultJobName, availableTaskNames)
	jobInput, err := j.askCreateQuestions(createQuestions)
	if err != nil {
		return model.JobSpec{}, err
	}

	cliMod, err := j.getPluginCliMod(pluginRepo, jobInput.Task.Name)
	if err != nil {
		return jobInput, err
	}
	if cliMod == nil {
		return jobInput, nil
	}

	pluginAnswers, err := j.askPluginQuestions(cliMod, jobInput.Name)
	if err != nil {
		return jobInput, err
	}

	taskConfig, err := j.getTaskConfig(cliMod, pluginAnswers)
	if err != nil {
		return jobInput, err
	}
	if taskConfig != nil {
		jobInput.Task.Config = taskConfig
	}

	asset, err := j.getJobAsset(cliMod, pluginAnswers)
	if err != nil {
		return jobInput, err
	}
	if asset != nil {
		jobInput.Asset = asset
	}
	return jobInput, nil
}

func (*JobCreateSurvey) getJobAsset(cliMod plugin.CommandLineMod, answers plugin.Answers) (map[string]string, error) {
	ctx := context.Background()
	defaultAssetRequest := plugin.DefaultAssetsRequest{Answers: answers}
	generatedAssetResponse, err := cliMod.DefaultAssets(ctx, defaultAssetRequest)
	if err != nil {
		return nil, err
	}
	var asset map[string]string
	if generatedAssetResponse.Assets != nil {
		asset = generatedAssetResponse.Assets.ToMap()
	}
	return asset, nil
}

func (*JobCreateSurvey) getTaskConfig(cliMod plugin.CommandLineMod, answers plugin.Answers) (map[string]string, error) {
	ctx := context.Background()
	defaultConfigRequest := plugin.DefaultConfigRequest{Answers: answers}
	generatedConfigResponse, err := cliMod.DefaultConfig(ctx, defaultConfigRequest)
	if err != nil {
		return nil, err
	}

	taskConfig := make(map[string]string)
	if generatedConfigResponse != nil {
		for _, conf := range generatedConfigResponse.Config {
			taskConfig[conf.Name] = conf.Value
		}
	}
	return taskConfig, nil
}

func (*JobCreateSurvey) getAvailableTaskNames(pluginRepo *models.PluginRepository) []string {
	plugins := pluginRepo.GetTasks()
	var output []string
	for _, task := range plugins {
		output = append(output, task.Info().Name)
	}
	return output
}

func (j *JobCreateSurvey) getCreateQuestions(jobSpecReader local.SpecReader[*model.JobSpec], jobDir, defaultJobName string, availableTaskNames []string) []*survey.Question {
	return []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the job name?",
				Default: defaultJobName,
				Help:    "It should be unique across whole optimus project",
			},
			Validate: survey.ComposeValidators(validateJobName, j.getValidateJobUniqueness(jobSpecReader, jobDir)),
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
				Message: "Select task to run?",
				Options: availableTaskNames,
				Help:    "Select the transformation task for this job",
			},
			Validate: survey.Required,
		},
		{
			Name: "start_date",
			Prompt: &survey.Input{
				Message: "Specify the schedule start date",
				Help:    "Format: (YYYY-MM-DD)",
				Default: time.Now().AddDate(0, 0, -1).UTC().Format(ISODateLayout),
			},
			Validate: validateDate,
		},
		{
			Name: "interval",
			Prompt: &survey.Input{
				Message: "Specify the schedule interval (in crontab notation)",
				Default: "0 2 * * *",
				Help: `0 2 * * * / @daily / @hourly
Note: remove interval field from job specification for manually triggered jobs`,
			},
			Validate: utils.ValidateCronInterval,
		},
		{
			Name: "window",
			Prompt: &survey.Select{
				Message: "Transformation window",
				Options: []string{"hourly", "daily", "weekly", "monthly"},
				Default: "daily",
				Help: `Time window for which transformation is consuming data,
this effects runtime dependencies and template macros`,
			},
		},
	}
}

func (j *JobCreateSurvey) askCreateQuestions(questions []*survey.Question) (model.JobSpec, error) {
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &baseInputsRaw); err != nil {
		return model.JobSpec{}, err
	}
	baseInputs, err := utils.ConvertToStringMap(baseInputsRaw)
	if err != nil {
		return model.JobSpec{}, err
	}

	return model.JobSpec{
		Version: jobSpecDefaultVersion,
		Name:    baseInputs["name"],
		Owner:   baseInputs["owner"],
		Schedule: model.JobSpecSchedule{
			StartDate: baseInputs["start_date"],
			Interval:  baseInputs["interval"],
		},
		Task: model.JobSpecTask{
			Name:   baseInputs["task"],
			Window: j.getWindowParameters(baseInputs["window"]),
		},
		Asset: map[string]string{},
		Behavior: model.JobSpecBehavior{
			DependsOnPast: false,
		},
		Dependencies: []model.JobSpecDependency{},
		Hooks:        []model.JobSpecHook{},
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
	}, nil
}

func (*JobCreateSurvey) getPluginCliMod(pluginRepo *models.PluginRepository, taskName string) (plugin.CommandLineMod, error) {
	plugin, err := pluginRepo.GetByName(taskName)
	if err != nil {
		return nil, err
	}
	return plugin.GetSurveyMod(), nil
}

func (j *JobCreateSurvey) askPluginQuestions(cliMod plugin.CommandLineMod, jobName string) (plugin.Answers, error) {
	ctx := context.Background()
	questionRequest := plugin.GetQuestionsRequest{JobName: jobName}
	questionResponse, err := cliMod.GetQuestions(ctx, questionRequest)
	if err != nil {
		return nil, err
	}

	answers := plugin.Answers{}
	for _, question := range questionResponse.Questions { //nolint: gocritic
		subAnswers, err := j.jobSurvey.askCliModSurveyQuestion(ctx, cliMod, question) //nolint: gocritic
		if err != nil {
			return nil, err
		}
		answers = append(answers, subAnswers...)
	}
	return answers, nil
}

// getValidateJobUniqueness return a validator that checks if the job already exists with the same name
func (*JobCreateSurvey) getValidateJobUniqueness(jobSpecReader local.SpecReader[*model.JobSpec], jobDir string) survey.Validator {
	return func(val interface{}) error {
		jobName, ok := val.(string)
		if !ok {
			return fmt.Errorf("invalid type of job name %v", reflect.TypeOf(val).Name())
		}
		if _, err := jobSpecReader.ReadByName(jobDir, jobName); err == nil {
			return fmt.Errorf("job with the provided name already exists")
		}
		return nil
	}
}

func (*JobCreateSurvey) getWindowParameters(winName string) model.JobSpecTaskWindow {
	switch winName {
	case "hourly":
		return model.JobSpecTaskWindow{
			Size:       "1h",
			Offset:     "0",
			TruncateTo: "h",
		}
	case "daily":
		return model.JobSpecTaskWindow{
			Size:       "24h",
			Offset:     "0",
			TruncateTo: "d",
		}
	case "weekly":
		return model.JobSpecTaskWindow{
			Size:       "168h",
			Offset:     "0",
			TruncateTo: "w",
		}
	case "monthly":
		return model.JobSpecTaskWindow{
			Size:       "720h",
			Offset:     "0",
			TruncateTo: "M",
		}
	}

	// default
	return model.JobSpecTaskWindow{
		Size:       "24h",
		Offset:     "0",
		TruncateTo: "h",
	}
}
