package survey

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/AlecAivazis/survey/v2"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
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
	specFileNames []string

	jobSurvey *JobSurvey
}

// NewJobCreateSurvey initializes job create survey
func NewJobCreateSurvey() *JobCreateSurvey {
	return &JobCreateSurvey{
		specFileNames: []string{local.ResourceSpecFileName, local.JobSpecFileName},
		jobSurvey:     NewJobSurvey(),
	}
}

// AskToCreateJob asks questions to create job
func (j *JobCreateSurvey) AskToCreateJob(jobSpecRepo JobSpecRepository, defaultJobName string) (local.Job, error) {
	availableTaskNames := j.getAvailableTaskNames()
	if len(availableTaskNames) == 0 {
		return local.Job{}, errors.New("no supported task plugin found")
	}

	createQuestions := j.getCreateQuestions(jobSpecRepo, defaultJobName, availableTaskNames)
	jobInput, err := j.askCreateQuestions(createQuestions)
	if err != nil {
		return local.Job{}, err
	}

	cliMod, err := j.getPluginCLIMod(jobInput.Task.Name)
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

func (j *JobCreateSurvey) getJobAsset(cliMod models.CommandLineMod, answers models.PluginAnswers) (map[string]string, error) {
	ctx := context.Background()
	defaultAssetRequest := models.DefaultAssetsRequest{Answers: answers}
	generatedAssetResponse, err := cliMod.DefaultAssets(ctx, defaultAssetRequest)
	if err != nil {
		return nil, err
	}
	var asset map[string]string
	if generatedAssetResponse.Assets != nil {
		asset = generatedAssetResponse.Assets.ToJobSpec().ToMap()
	}
	return asset, nil
}

func (j *JobCreateSurvey) getTaskConfig(cliMod models.CommandLineMod, answers models.PluginAnswers) (yaml.MapSlice, error) {
	ctx := context.Background()
	defaultConfigRequest := models.DefaultConfigRequest{Answers: answers}
	generatedConfigResponse, err := cliMod.DefaultConfig(ctx, defaultConfigRequest)
	if err != nil {
		return nil, err
	}
	var taskConfig yaml.MapSlice
	if generatedConfigResponse.Config != nil {
		jobSpecConfigs := generatedConfigResponse.Config.ToJobSpec()
		taskConfig = local.JobSpecConfigToYamlSlice(jobSpecConfigs)
	}
	return taskConfig, nil
}

func (j *JobCreateSurvey) getAvailableTaskNames() []string {
	pluginRepo := models.PluginRegistry
	var output []string
	for _, task := range pluginRepo.GetTasks() {
		output = append(output, task.Info().Name)
	}
	return output
}

func (j *JobCreateSurvey) getCreateQuestions(jobSpecRepo JobSpecRepository, defaultJobName string, availableTaskNames []string) []*survey.Question {
	return []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the job name?",
				Default: defaultJobName,
				Help:    "It should be unique across whole optimus project",
			},
			Validate: survey.ComposeValidators(validateJobName, j.getValidateJobUniqueness(jobSpecRepo)),
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
				Default: time.Now().AddDate(0, 0, -1).UTC().Format(models.JobDatetimeLayout),
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

func (j *JobCreateSurvey) askCreateQuestions(questions []*survey.Question) (local.Job, error) {
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &baseInputsRaw); err != nil {
		return local.Job{}, err
	}
	baseInputs, err := utils.ConvertToStringMap(baseInputsRaw)
	if err != nil {
		return local.Job{}, err
	}

	return local.Job{
		Version: local.JobConfigVersion,
		Name:    baseInputs["name"],
		Owner:   baseInputs["owner"],
		Schedule: local.JobSchedule{
			StartDate: baseInputs["start_date"],
			Interval:  baseInputs["interval"],
		},
		Task: local.JobTask{
			Name:   baseInputs["task"],
			Window: j.getWindowParameters(baseInputs["window"]),
		},
		Asset: map[string]string{},
		Behavior: local.JobBehavior{
			Catchup:       true,
			DependsOnPast: false,
		},
		Dependencies: []local.JobDependency{},
		Hooks:        []local.JobHook{},
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
	}, nil
}

func (j *JobCreateSurvey) getPluginCLIMod(taskName string) (models.CommandLineMod, error) {
	pluginRepo := models.PluginRegistry
	executionTask, err := pluginRepo.GetByName(taskName)
	if err != nil {
		return nil, err
	}
	return executionTask.CLIMod, nil
}

func (j *JobCreateSurvey) askPluginQuestions(cliMod models.CommandLineMod, jobName string) (models.PluginAnswers, error) {
	ctx := context.Background()
	questionRequest := models.GetQuestionsRequest{JobName: jobName}
	questionResponse, err := cliMod.GetQuestions(ctx, questionRequest)
	if err != nil {
		return nil, err
	}

	answers := models.PluginAnswers{}
	for _, question := range questionResponse.Questions {
		answers, err := j.jobSurvey.askCLIModSurveyQuestion(cliMod, question)
		if err != nil {
			return nil, err
		}
		answers = append(answers, answers...)
	}
	return answers, nil
}

// getValidateJobUniqueness return a validator that checks if the job already exists with the same name
func (j *JobCreateSurvey) getValidateJobUniqueness(repository JobSpecRepository) survey.Validator {
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

func (j *JobCreateSurvey) getWindowParameters(winName string) local.JobTaskWindow {
	switch winName {
	case "hourly":
		return local.JobTaskWindow{
			Size:       "1h",
			Offset:     "0",
			TruncateTo: "h",
		}
	case "daily":
		return local.JobTaskWindow{
			Size:       "24h",
			Offset:     "0",
			TruncateTo: "h",
		}
	case "weekly":
		return local.JobTaskWindow{
			Size:       "168h",
			Offset:     "0",
			TruncateTo: "w",
		}
	case "monthly":
		return local.JobTaskWindow{
			Size:       "720h",
			Offset:     "0",
			TruncateTo: "M",
		}
	}

	// default
	return local.JobTaskWindow{
		Size:       "24h",
		Offset:     "0",
		TruncateTo: "h",
	}
}

// AskWorkingDirectory asks and returns the directory where the new spec folder should be created
func (j *JobCreateSurvey) AskWorkingDirectory(jobSpecFs afero.Fs, root string) (string, error) {
	directories, err := afero.ReadDir(jobSpecFs, root)
	if err != nil {
		return "", err
	}
	if len(directories) == 0 {
		return root, nil
	}

	currentFolder := ". (current directory)"

	availableDirs := []string{currentFolder}
	for _, dir := range directories {
		if !dir.IsDir() {
			continue
		}

		// if it contains job or resource, skip it from valid options
		dirItems, err := afero.ReadDir(jobSpecFs, filepath.Join(root, dir.Name()))
		if err != nil {
			return "", err
		}
		var alreadyOccupied bool
		for _, dirItem := range dirItems {
			if utils.ContainsString(j.specFileNames, dirItem.Name()) {
				alreadyOccupied = true
				break
			}
		}
		if alreadyOccupied {
			continue
		}
		availableDirs = append(availableDirs, dir.Name())
	}

	messageStr := "Select directory to save specification?"
	if root != "" {
		messageStr = fmt.Sprintf("%s [%s]", messageStr, root)
	}
	var selectedDir string
	if err := survey.AskOne(&survey.Select{
		Message: messageStr,
		Default: currentFolder,
		Help:    "Optimus helps organize specifications in sub-directories.\nPlease select where you want this new specification to be stored",
		Options: availableDirs,
	}, &selectedDir); err != nil {
		return "", err
	}

	// check for sub-directories
	if selectedDir != currentFolder {
		return j.AskWorkingDirectory(jobSpecFs, filepath.Join(root, selectedDir))
	}
	return root, nil
}

// AskDirectoryName asks and returns the directory name of the new spec folder
func (j *JobCreateSurvey) AskDirectoryName(root string) (string, error) {
	numberOfWordsToGenerate := 2
	sampleDirectoryName := petname.Generate(numberOfWordsToGenerate, "_")

	var selectedDir string
	if err := survey.AskOne(&survey.Input{
		Message: fmt.Sprintf("Provide new directory name to create for this spec?[%s/.]", root),
		Default: sampleDirectoryName,
		Help:    fmt.Sprintf("A new directory will be created under '%s/%s'", root, sampleDirectoryName),
	}, &selectedDir); err != nil {
		return "", err
	}

	return selectedDir, nil
}
