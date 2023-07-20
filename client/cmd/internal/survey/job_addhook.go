package survey

import (
	"context"
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"

	"github.com/raystack/optimus/client/local/model"
	"github.com/raystack/optimus/internal/models"
	"github.com/raystack/optimus/sdk/plugin"
)

// JobAddHookSurvey defines survey for job add hook
type JobAddHookSurvey struct {
	jobSurvey *JobSurvey
}

// NewJobAddHookSurvey initializes job add hook survey
func NewJobAddHookSurvey() *JobAddHookSurvey {
	return &JobAddHookSurvey{
		jobSurvey: NewJobSurvey(),
	}
}

// AskToAddHook asks questions to add hook to a job
func (j *JobAddHookSurvey) AskToAddHook(pluginRepo *models.PluginRepository, jobSpec *model.JobSpec) (*model.JobSpec, error) {
	newJobSpec := *jobSpec
	availableHookNames := j.getAvailableHookNames(pluginRepo)
	if len(availableHookNames) == 0 {
		return nil, errors.New("no supported hook plugin found")
	}

	selectedHookName, err := j.askToSelectHook(availableHookNames)
	if err != nil {
		return nil, err
	}

	if j.isSelectedHookAlreadyInJob(jobSpec, selectedHookName) {
		return nil, fmt.Errorf("hook %s already exists for this job", selectedHookName)
	}

	selectedHook, err := pluginRepo.GetByName(selectedHookName)
	if err != nil {
		return nil, err
	}

	var config map[string]string
	if cliMod := selectedHook.GetSurveyMod(); cliMod != nil {
		ctx := context.Background()
		hookAnswers, err := j.askHookQuestions(ctx, cliMod, jobSpec.Name)
		if err != nil {
			return nil, err
		}

		config, err = j.getHookConfig(cliMod, hookAnswers)
		if err != nil {
			return nil, err
		}
	}
	// TODO: remove the golint exception below
	newJobSpec.Hooks = append(jobSpec.Hooks, model.JobSpecHook{ //nolint:gocritic
		Name:   selectedHook.Info().Name,
		Config: config,
	})
	return &newJobSpec, nil
}

func (*JobAddHookSurvey) getHookConfig(cliMod plugin.CommandLineMod, answers plugin.Answers) (map[string]string, error) {
	ctx := context.Background()
	configRequest := plugin.DefaultConfigRequest{Answers: answers}
	generatedConfigResponse, err := cliMod.DefaultConfig(ctx, configRequest)
	if err != nil {
		return nil, err
	}

	config := map[string]string{}
	if generatedConfigResponse != nil {
		for _, cfg := range generatedConfigResponse.Config {
			config[cfg.Name] = cfg.Value
		}
	}

	return config, nil
}

func (*JobAddHookSurvey) getAvailableHookNames(pluginRepo *models.PluginRepository) []string {
	var output []string
	for _, hook := range pluginRepo.GetHooks() {
		output = append(output, hook.Info().Name)
	}
	return output
}

func (*JobAddHookSurvey) askToSelectHook(options []string) (string, error) {
	question := &survey.Select{
		Message: "Select hook to attach?",
		Options: options,
	}
	var answer string
	if err := survey.AskOne(question, &answer); err != nil {
		return "", err
	}
	return answer, nil
}

func (*JobAddHookSurvey) isSelectedHookAlreadyInJob(jobSpec *model.JobSpec, selectedHookName string) bool {
	for _, hook := range jobSpec.Hooks {
		if hook.Name == selectedHookName {
			return true
		}
	}
	return false
}

func (j *JobAddHookSurvey) askHookQuestions(ctx context.Context, cliMod plugin.CommandLineMod, jobName string) (plugin.Answers, error) {
	questionRequest := plugin.GetQuestionsRequest{JobName: jobName}
	questionResponse, err := cliMod.GetQuestions(ctx, questionRequest)
	if err != nil {
		return nil, err
	}

	answers := plugin.Answers{}
	for _, question := range questionResponse.Questions { //nolint: gocritic
		responseAnswer, err := j.jobSurvey.askCliModSurveyQuestion(ctx, cliMod, question)
		if err != nil {
			return nil, err
		}
		answers = append(answers, responseAnswer...)
	}
	return answers, nil
}
