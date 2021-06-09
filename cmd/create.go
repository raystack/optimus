package cmd

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/odpf/optimus/core/fs"

	"github.com/odpf/optimus/utils"

	"strconv"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
)

var (
	validateDate         = utils.ValidatorFactory.NewFromRegex(`\d{4}-\d{2}-\d{2}`, "date must be in YYYY-MM-DD format")
	validateNoSlash      = utils.ValidatorFactory.NewFromRegex(`^[^/]+$`, "`/` is disallowed")
	validateResourceName = utils.ValidatorFactory.NewFromRegex(`^[a-zA-Z0-9][a-zA-Z0-9_\-\.]+$`,
		`invalid name (can only contain characters A-Z (in either case), 0-9, "-", "_" or "." and must start with an alphanumeric character)`)
	validateJobName = survey.ComposeValidators(validateNoSlash, validateResourceName, survey.MinLength(3),
		survey.MaxLength(1024))
)

func createCommand(l logger, jobSpecRepo store.JobSpecRepository,
	taskRepo models.TaskPluginRepository, hookRepo models.HookRepo, datastoreRepo models.DatastoreRepo,
	datastoreSpecsFs map[string]fs.FileSystem) *cli.Command {
	cmd := &cli.Command{
		Use:   "create",
		Short: "Create a new job/resource",
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(createJobSubCommand(l, jobSpecRepo, taskRepo, hookRepo))
		cmd.AddCommand(createHookSubCommand(l, jobSpecRepo, hookRepo))
	}
	cmd.AddCommand(createResourceSubCommand(l, datastoreSpecsFs, datastoreRepo))
	return cmd
}

func createJobSubCommand(l logger, jobSpecRepo store.JobSpecRepository, taskRepo models.TaskPluginRepository,
	hookRepo models.HookRepo) *cli.Command {
	return &cli.Command{
		Use:   "job",
		Short: "create a new Job",
		RunE: func(cmd *cli.Command, args []string) error {
			jobInput, err := createJobSurvey(jobSpecRepo, taskRepo)
			if err != nil {
				return err
			}
			spec, err := local.NewJobSpecAdapter(taskRepo, hookRepo).ToSpec(jobInput)
			if err != nil {
				return err
			}
			if err := jobSpecRepo.Save(spec); err != nil {
				return err
			}
			l.Println("job created successfully", spec.Name)
			return nil
		},
	}
}

func createJobSurvey(jobSpecRepo store.JobSpecRepository, taskRepo models.TaskPluginRepository) (local.Job, error) {
	// TODO: take an additional input "--spec-dir" with default as "." in order to save job specs to a specific directory
	availableTasks := []string{}
	for _, task := range taskRepo.GetAll() {
		schema, err := task.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
		if err != nil {
			return local.Job{}, err
		}
		availableTasks = append(availableTasks, schema.Name)
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

	executionTask, err := taskRepo.GetByName(jobInput.Task.Name)
	if err != nil {
		return jobInput, err
	}

	taskQuesResponse, err := executionTask.GetTaskQuestions(context.TODO(), models.GetTaskQuestionsRequest{})
	if err != nil {
		return jobInput, err
	}

	userInputs := models.PluginAnswers{}
	for _, ques := range taskQuesResponse.Questions {
		responseAnswer, err := AskTaskSurveyQuestion(ques, executionTask)
		if err != nil {
			return local.Job{}, err
		}
		userInputs = append(userInputs, responseAnswer...)
	}

	generateConfResponse, err := executionTask.DefaultTaskConfig(context.TODO(), models.DefaultTaskConfigRequest{
		Answers: userInputs,
	})
	if err != nil {
		return jobInput, err
	}
	jobInput.Task.Config = local.JobSpecConfigToYamlSlice(generateConfResponse.Config.ToJobSpec())

	genAssetResponse, err := executionTask.DefaultTaskAssets(context.TODO(), models.DefaultTaskAssetsRequest{
		Answers: userInputs,
	})
	if err != nil {
		return jobInput, err
	}
	jobInput.Asset = genAssetResponse.Assets.ToJobSpec().ToMap()

	return jobInput, nil
}

func createHookSubCommand(l logger, jobSpecRepo store.JobSpecRepository, hookRepo models.HookRepo) *cli.Command {
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
			jobSpec, err = createHookSurvey(jobSpec, hookRepo)
			if err != nil {
				return err
			}
			return jobSpecRepo.Save(jobSpec)
		},
	}
	return cmd
}

func createHookSurvey(jobSpec models.JobSpec, hookRepo models.HookRepo) (models.JobSpec, error) {
	emptyJobSpec := models.JobSpec{}
	var availableHooks []string
	for _, hook := range hookRepo.GetAll() {
		schema, err := hook.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
		if err != nil {
			return models.JobSpec{}, err
		}

		// TODO: this should be generated at runtime based on what base task is
		// selected, support it when we add more than one type of task
		availableHooks = append(availableHooks, schema.Name)
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
	baseInputs, err := utils.ConvertToStringMap(baseInputsRaw)
	if err != nil {
		return emptyJobSpec, err
	}

	selectedHook := baseInputs["hook"]
	if ifHookAlreadyExistsForJob(jobSpec, selectedHook) {
		return emptyJobSpec, errors.Errorf("hook %s already exists for this job", selectedHook)
	}

	executionHook, err := hookRepo.GetByName(selectedHook)
	if err != nil {
		return emptyJobSpec, err
	}

	taskQuesResponse, err := executionHook.GetHookQuestions(context.TODO(), models.GetHookQuestionsRequest{})
	if err != nil {
		return emptyJobSpec, err
	}

	userInputs := models.PluginAnswers{}
	for _, ques := range taskQuesResponse.Questions {
		responseAnswer, err := AskHookSurveyQuestion(ques, executionHook)
		if err != nil {
			return emptyJobSpec, err
		}
		userInputs = append(userInputs, responseAnswer...)
	}

	generateConfResponse, err := executionHook.DefaultHookConfig(context.TODO(), models.DefaultHookConfigRequest{
		Answers:    userInputs,
		TaskConfig: models.TaskPluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
	})
	if err != nil {
		return emptyJobSpec, err
	}

	jobSpec.Hooks = append(jobSpec.Hooks, models.JobSpecHook{
		Unit:   executionHook,
		Config: generateConfResponse.Config.ToJobSpec(),
	})
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
		schema, err := hook.Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
		if err != nil {
			return false
		}
		if schema.Name == newHookName {
			return true
		}
	}
	return false
}

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

func createResourceSubCommand(l logger, datastoreSpecFs map[string]fs.FileSystem, datastoreRepo models.DatastoreRepo) *cli.Command {
	return &cli.Command{
		Use:   "resource",
		Short: "create a new resource",
		RunE: func(cmd *cli.Command, args []string) error {
			availableStorer := []string{}
			for _, s := range datastoreRepo.GetAll() {
				availableStorer = append(availableStorer, s.Name())
			}
			var storerName string
			if err := survey.AskOne(&survey.Select{
				Message: "Select supported datastores?",
				Options: availableStorer,
			}, &storerName); err != nil {
				return err
			}
			repoFS, ok := datastoreSpecFs[storerName]
			if !ok {
				return fmt.Errorf("unregistered datastore, please use configuration file to set datastore path")
			}

			availableTypes := []string{}
			datastore, _ := datastoreRepo.GetByName(storerName)
			for dsType := range datastore.Types() {
				availableTypes = append(availableTypes, dsType.String())
			}
			resourceSpecRepo := local.NewResourceSpecRepository(repoFS, datastore)

			var resourceType string
			if err := survey.AskOne(&survey.Select{
				Message: "Select supported resource type?",
				Options: availableTypes,
			}, &resourceType); err != nil {
				return err
			}
			typeController, _ := datastore.Types()[models.ResourceType(resourceType)]

			var qs = []*survey.Question{
				{
					Name: "name",
					Prompt: &survey.Input{
						Message: "What is the resource name?(should conform to selected resource type)",
					},
					Validate: survey.ComposeValidators(validateNoSlash, survey.MinLength(3),
						survey.MaxLength(1024), IsValidDatastoreSpec(typeController.Validator()),
						IsResourceNameUnique(resourceSpecRepo)),
				},
			}
			inputs := map[string]interface{}{}
			if err := survey.Ask(qs, &inputs); err != nil {
				return err
			}
			resourceName := inputs["name"].(string)

			if err := resourceSpecRepo.Save(models.ResourceSpec{
				Version:   1,
				Name:      resourceName,
				Type:      models.ResourceType(resourceType),
				Datastore: datastore,
				Assets:    typeController.DefaultAssets(),
			}); err != nil {
				return err
			}
			l.Println("resource created successfully", resourceName)

			return nil
		},
	}
}

// IsResourceNameUnique return a validator that checks if the resource already exists with the same name
func IsResourceNameUnique(repository store.ResourceSpecRepository) survey.Validator {
	return func(val interface{}) error {
		if str, ok := val.(string); ok {
			if _, err := repository.GetByName(str); err == nil {
				return fmt.Errorf("resource with the provided name already exists")
			} else if err != models.ErrNoSuchSpec && err != models.ErrNoResources {
				return err
			}
		} else {
			// otherwise we cannot convert the value into a string and cannot find a resource name
			return fmt.Errorf("invalid type of resource name %v", reflect.TypeOf(val).Name())
		}
		// the input is fine
		return nil
	}
}

// IsValidDatastoreSpec tries to adapt provided resource with datastore
func IsValidDatastoreSpec(valiFn models.DatastoreSpecValidator) survey.Validator {
	return func(val interface{}) error {
		if str, ok := val.(string); ok {
			if err := valiFn(models.ResourceSpec{
				Name: str,
			}); err != nil {
				return err
			}
		} else {
			// otherwise we cannot convert the value into a string and cannot find a resource name
			return fmt.Errorf("invalid type of resource name %v", reflect.TypeOf(val).Name())
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

func AskTaskSurveyQuestion(ques models.PluginQuestion, execUnit models.TaskPlugin) (models.PluginAnswers, error) {
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
		resp, err := execUnit.ValidateTaskQuestion(context.TODO(), models.ValidateTaskQuestionRequest{
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
	if len(ques.SubQuestions) > 0 && responseStr == ques.SubQuestionsIfValue {
		for _, subQues := range ques.SubQuestions {
			subQuesAnswer, err := AskTaskSurveyQuestion(subQues, execUnit)
			if err != nil {
				return nil, err
			}
			answers = append(answers, subQuesAnswer...)
		}
	}

	return answers, nil
}

func AskHookSurveyQuestion(ques models.PluginQuestion, execUnit models.HookPlugin) (models.PluginAnswers, error) {
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
		resp, err := execUnit.ValidateHookQuestion(context.TODO(), models.ValidateHookQuestionRequest{
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
		return nil, errors.Wrap(err, "AskHookSurveyQuestion")
	}

	answers := models.PluginAnswers{
		models.PluginAnswer{
			Question: ques,
			Value:    responseStr,
		},
	}

	// check if sub questions are attached on this question
	if len(ques.SubQuestions) > 0 && responseStr == ques.SubQuestionsIfValue {
		for _, subQues := range ques.SubQuestions {
			subQuesAnswer, err := AskHookSurveyQuestion(subQues, execUnit)
			if err != nil {
				return nil, err
			}
			answers = append(answers, subQuesAnswer...)
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
