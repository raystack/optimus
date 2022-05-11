package survey

import (
	"context"
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"

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
func (j *JobAddHookSurvey) AskToAddHook(jobSpec models.JobSpec, pluginRepo models.PluginRepository) (models.JobSpec, error) {
	availableHookNames := j.getAvailableHookNames()
	if len(availableHookNames) == 0 {
		return models.JobSpec{}, errors.New("no supported hook plugin found")
	}

	selectedHookName, err := j.askToSelectHook(availableHookNames)
	if err != nil {
		return models.JobSpec{}, err
	}

	if j.isSelectedHookAlreadyInJob(jobSpec, selectedHookName) {
		return models.JobSpec{}, fmt.Errorf("hook %s already exists for this job", selectedHookName)
	}

	selectedHook, err := pluginRepo.GetByName(selectedHookName)
	if err != nil {
		return models.JobSpec{}, err
	}

	var jobSpecConfigs models.JobSpecConfigs
	if cliMod := selectedHook.CLIMod; cliMod != nil {
		ctx := context.Background()
		hookAnswers, err := j.askHookQuestions(ctx, cliMod, jobSpec.Name)
		if err != nil {
			return models.JobSpec{}, err
		}

		jobSpecConfigs, err = j.getHookConfig(cliMod, hookAnswers)
		if err != nil {
			return models.JobSpec{}, err
		}
	}
	jobSpec.Hooks = append(jobSpec.Hooks, models.JobSpecHook{
		Unit:   selectedHook,
		Config: jobSpecConfigs,
	})
	return jobSpec, nil
}

func (*JobAddHookSurvey) getHookConfig(cliMod models.CommandLineMod, answers models.PluginAnswers) (models.JobSpecConfigs, error) {
	ctx := context.Background()
	configRequest := models.DefaultConfigRequest{Answers: answers}
	generatedConfigResponse, err := cliMod.DefaultConfig(ctx, configRequest)
	if err != nil {
		return nil, err
	}
	var config models.JobSpecConfigs
	if generatedConfigResponse.Config != nil {
		config = generatedConfigResponse.Config.ToJobSpec()
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

func (*JobAddHookSurvey) isSelectedHookAlreadyInJob(jobSpec models.JobSpec, selectedHookName string) bool {
	for _, hook := range jobSpec.Hooks {
		if hook.Unit.Info().Name == selectedHookName {
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
		responseAnswer, err := j.jobSurvey.askCLIModSurveyQuestion(cliMod, question)
		if err != nil {
			return nil, err
		}
		answers = append(answers, responseAnswer...)
	}
	return answers, nil
}
