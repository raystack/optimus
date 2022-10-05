package survey

import (
	"context"
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"

	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/models"
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
func (j *JobAddHookSurvey) AskToAddHook(jobSpec *local.JobSpec) (*local.JobSpec, error) {
	pluginRepo := models.PluginRegistry
	newJobSpec := *jobSpec
	availableHookNames := j.getAvailableHookNames()
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
	newJobSpec.Hooks = append(jobSpec.Hooks, local.JobSpecHook{
		Name:   selectedHook.Info().Name,
		Config: config,
	})
	return &newJobSpec, nil
}

func (*JobAddHookSurvey) getHookConfig(cliMod models.CommandLineMod, answers models.PluginAnswers) (map[string]string, error) {
	ctx := context.Background()
	configRequest := models.DefaultConfigRequest{Answers: answers}
	generatedConfigResponse, err := cliMod.DefaultConfig(ctx, configRequest)
	if err != nil {
		return nil, err
	}
	var jobSpecConfig models.JobSpecConfigs
	if generatedConfigResponse.Config != nil {
		jobSpecConfig = generatedConfigResponse.Config.ToJobSpec()
	}

	config := map[string]string{}
	for _, cfg := range jobSpecConfig {
		config[cfg.Name] = cfg.Value
	}
	return config, nil
}

func (*JobAddHookSurvey) getAvailableHookNames() []string {
	pluginRepo := models.PluginRegistry
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

func (*JobAddHookSurvey) isSelectedHookAlreadyInJob(jobSpec *local.JobSpec, selectedHookName string) bool {
	for _, hook := range jobSpec.Hooks {
		if hook.Name == selectedHookName {
			return true
		}
	}
	return false
}

func (j *JobAddHookSurvey) askHookQuestions(ctx context.Context, cliMod models.CommandLineMod, jobName string) (models.PluginAnswers, error) {
	questionRequest := models.GetQuestionsRequest{JobName: jobName}
	questionResponse, err := cliMod.GetQuestions(ctx, questionRequest)
	if err != nil {
		return nil, err
	}

	answers := models.PluginAnswers{}
	for _, question := range questionResponse.Questions {
		responseAnswer, err := j.jobSurvey.askCLIModSurveyQuestion(ctx, cliMod, question)
		if err != nil {
			return nil, err
		}
		answers = append(answers, responseAnswer...)
	}
	return answers, nil
}
