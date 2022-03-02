package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
)

const GeneratedNameWords = 2

var (
	validateDate    = utils.ValidatorFactory.NewFromRegex(`\d{4}-\d{2}-\d{2}`, "date must be in YYYY-MM-DD format")
	validateNoSlash = utils.ValidatorFactory.NewFromRegex(`^[^/]+$`, "`/` is disallowed")
	validateJobName = survey.ComposeValidators(validateNoSlash, validateResourceName, survey.MinLength(3),
		survey.MaxLength(220))

	specFileNames = []string{local.ResourceSpecFileName, local.JobSpecFileName}
)

func jobCreateCommand(l log.Logger, jobSpecFs afero.Fs, jobSpecRepo JobSpecRepository,
	pluginRepo models.PluginRepository) *cli.Command {
	return &cli.Command{
		Use:     "create",
		Short:   "Create a new Job",
		Example: "optimus job create",
		RunE: func(cmd *cli.Command, args []string) error {
			jwd, err := getWorkingDirectory(jobSpecFs, "")
			if err != nil {
				return err
			}
			newDirName, err := getDirectoryName(jwd)
			if err != nil {
				return err
			}

			jobDirectory := filepath.Join(jwd, newDirName)
			jobNameDefault := strings.ReplaceAll(strings.ReplaceAll(jobDirectory, "/", "."), "\\", ".")

			jobInput, err := createJobSurvey(jobSpecRepo, pluginRepo, jobNameDefault)
			if err != nil {
				return err
			}
			spec, err := local.NewJobSpecAdapter(pluginRepo).ToSpec(jobInput)
			if err != nil {
				return err
			}
			if err := jobSpecRepo.SaveAt(spec, jobDirectory); err != nil {
				return err
			}
			l.Info(coloredSuccess("Job successfully created at %s", jobDirectory))
			return nil
		},
	}
}

// getWorkingDirectory returns the directory where the new spec folder should be created
func getWorkingDirectory(jobSpecFs afero.Fs, root string) (string, error) {
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
			if utils.ContainsString(specFileNames, dirItem.Name()) {
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
	if err = survey.AskOne(&survey.Select{
		Message: messageStr,
		Default: currentFolder,
		Help:    "Optimus helps organize specifications in sub-directories.\nPlease select where you want this new specification to be stored",
		Options: availableDirs,
	}, &selectedDir); err != nil {
		return "", err
	}

	// check for sub-directories
	if selectedDir != currentFolder {
		return getWorkingDirectory(jobSpecFs, filepath.Join(root, selectedDir))
	}

	return root, nil
}

// getDirectoryName returns the directory name of the new spec folder
func getDirectoryName(root string) (string, error) {
	sampleDirectoryName := petname.Generate(GeneratedNameWords, "_")

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

func createJobSurvey(jobSpecRepo JobSpecRepository, pluginRepo models.PluginRepository,
	jobNameDefault string) (local.Job, error) {
	var availableTasks []string
	for _, task := range pluginRepo.GetTasks() {
		availableTasks = append(availableTasks, task.Info().Name)
	}
	if len(availableTasks) == 0 {
		return local.Job{}, errors.New("no supported task plugin found")
	}

	var qs = []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the job name?",
				Default: jobNameDefault,
				Help:    "It should be unique across whole optimus project",
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
				Message: "Select task to run?",
				Options: availableTasks,
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
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(qs, &baseInputsRaw); err != nil {
		return local.Job{}, err
	}
	baseInputs, err := utils.ConvertToStringMap(baseInputsRaw)
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
			Window: getWindowParameters(baseInputs["window"]),
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
	}

	executionTask, err := pluginRepo.GetByName(jobInput.Task.Name)
	if err != nil {
		return jobInput, err
	}

	cliMod := executionTask.CLIMod
	if cliMod == nil {
		return jobInput, nil
	}

	taskQuesResponse, err := cliMod.GetQuestions(context.Background(), models.GetQuestionsRequest{
		JobName: jobInput.Name,
	})
	if err != nil {
		return jobInput, err
	}

	userInputs := models.PluginAnswers{}
	if taskQuesResponse.Questions != nil {
		for _, ques := range taskQuesResponse.Questions {
			responseAnswer, err := AskCLISurveyQuestion(ques, cliMod)
			if err != nil {
				return local.Job{}, err
			}
			userInputs = append(userInputs, responseAnswer...)
		}
	}

	generateConfResponse, err := cliMod.DefaultConfig(context.Background(), models.DefaultConfigRequest{
		Answers: userInputs,
	})
	if err != nil {
		return jobInput, err
	}
	if generateConfResponse.Config != nil {
		jobInput.Task.Config = local.JobSpecConfigToYamlSlice(generateConfResponse.Config.ToJobSpec())
	}

	genAssetResponse, err := cliMod.DefaultAssets(context.Background(), models.DefaultAssetsRequest{
		Answers: userInputs,
	})
	if err != nil {
		return jobInput, err
	}
	if genAssetResponse.Assets != nil {
		jobInput.Asset = genAssetResponse.Assets.ToJobSpec().ToMap()
	}

	return jobInput, nil
}

// selectJobSurvey runs a survey to select a job and returns its name
func selectJobSurvey(jobSpecRepo JobSpecRepository) (string, error) {
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

// IsJobNameUnique return a validator that checks if the job already exists with the same name
func IsJobNameUnique(repository JobSpecRepository) survey.Validator {
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

func getWindowParameters(winName string) local.JobTaskWindow {
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

	//default
	return local.JobTaskWindow{
		Size:       "24h",
		Offset:     "0",
		TruncateTo: "h",
	}
}

func AskCLISurveyQuestion(ques models.PluginQuestion, cliMod models.CommandLineMod) (models.PluginAnswers, error) {
	var surveyPrompt survey.Prompt
	if len(ques.Multiselect) > 0 {
		sel := &survey.Select{
			Message: ques.Prompt,
			Help:    ques.Help,
			Options: ques.Multiselect,
		}
		if len(ques.Default) > 0 {
			sel.Default = ques.Default
		}
		surveyPrompt = sel
	} else {
		sel := &survey.Input{
			Message: ques.Prompt,
			Help:    ques.Help,
		}
		if len(ques.Default) > 0 {
			sel.Default = ques.Default
		}
		surveyPrompt = sel
	}
	var responseStr string
	if err := survey.AskOne(surveyPrompt, &responseStr, survey.WithValidator(func(val interface{}) error {
		str, err := ConvertUserInputToString(val)
		if err != nil {
			return err
		}
		resp, err := cliMod.ValidateQuestion(context.TODO(), models.ValidateQuestionRequest{
			Answer: models.PluginAnswer{
				Question: ques,
				Value:    str,
			},
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return errors.New(resp.Error)
		}
		return nil
	})); err != nil {
		return nil, errors.Wrap(err, "AskSurveyQuestion")
	}

	answers := models.PluginAnswers{
		models.PluginAnswer{
			Question: ques,
			Value:    responseStr,
		},
	}

	// check if sub questions are attached on this question
	for _, subQues := range ques.SubQuestions {
		if responseStr == subQues.IfValue {
			for _, subQ := range subQues.Questions {
				subQuesAnswer, err := AskCLISurveyQuestion(subQ, cliMod)
				if err != nil {
					return nil, err
				}
				answers = append(answers, subQuesAnswer...)
			}
		}
	}

	return answers, nil
}

func ConvertUserInputToString(val interface{}) (string, error) {
	var responseStr string
	switch reflect.TypeOf(val).Name() {
	case "int":
		responseStr = strconv.Itoa(val.(int))
	case "string":
		responseStr = val.(string)
	case "OptionAnswer":
		responseStr = val.(survey.OptionAnswer).Value
	default:
		return "", errors.Errorf("unknown type found while parsing input: %v", val)
	}
	return responseStr, nil
}
