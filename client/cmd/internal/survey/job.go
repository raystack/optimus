package survey

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/AlecAivazis/survey/v2"

	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/client/local/model"
	"github.com/odpf/optimus/internal/models"
)

// JobSurvey defines survey for job specification in general
type JobSurvey struct {
}

// NewJobSurvey initializes job survey
func NewJobSurvey() *JobSurvey {
	return &JobSurvey{}
}

// AskToSelectJobName asks to select job name
func (*JobSurvey) AskToSelectJobName(jobSpecReader local.SpecReader[*model.JobSpec], jobDirPath string) (string, error) {
	jobs, err := jobSpecReader.ReadAll(jobDirPath)
	if err != nil {
		return "", err
	}
	var allJobNames []string
	for _, job := range jobs {
		allJobNames = append(allJobNames, job.Name)
	}
	var selectedJobName string
	if err := survey.AskOne(&survey.Select{
		Message: "Select a Job",
		Options: allJobNames,
	}, &selectedJobName); err != nil {
		return "", err
	}
	return selectedJobName, nil
}

func (j *JobSurvey) askCliModSurveyQuestion(ctx context.Context, cliMod models.CommandLineMod, question models.PluginQuestion) (models.PluginAnswers, error) {
	surveyPrompt := j.getSurveyPromptFromPluginQuestion(question)

	var responseStr string
	if err := survey.AskOne(
		surveyPrompt,
		&responseStr,
		survey.WithValidator(j.getValidatePluginQuestion(ctx, cliMod, question)),
	); err != nil {
		return nil, fmt.Errorf("AskSurveyQuestion: %w", err)
	}

	answers := models.PluginAnswers{
		models.PluginAnswer{
			Question: question,
			Value:    responseStr,
		},
	}

	// check if sub questions are attached on this question
	for _, subQues := range question.SubQuestions {
		if responseStr == subQues.IfValue {
			for _, subQuestion := range subQues.Questions {
				subQuestionAnswers, err := j.askCliModSurveyQuestion(ctx, cliMod, subQuestion)
				if err != nil {
					return nil, err
				}
				answers = append(answers, subQuestionAnswers...)
			}
		}
	}

	return answers, nil
}

func (*JobSurvey) getSurveyPromptFromPluginQuestion(question models.PluginQuestion) survey.Prompt {
	var surveyPrompt survey.Prompt
	if len(question.Multiselect) > 0 {
		sel := &survey.Select{
			Message: question.Prompt,
			Help:    question.Help,
			Options: question.Multiselect,
		}
		if len(question.Default) > 0 {
			sel.Default = question.Default
		}
		surveyPrompt = sel
	} else {
		sel := &survey.Input{
			Message: question.Prompt,
			Help:    question.Help,
		}
		if len(question.Default) > 0 {
			sel.Default = question.Default
		}
		surveyPrompt = sel
	}
	return surveyPrompt
}

func (j *JobSurvey) getValidatePluginQuestion(ctx context.Context, cliMod models.CommandLineMod, question models.PluginQuestion) survey.Validator {
	return func(val interface{}) error {
		str, err := j.convertUserInputPluginToString(val)
		if err != nil {
			return err
		}
		resp, err := cliMod.ValidateQuestion(ctx, models.ValidateQuestionRequest{
			Answer: models.PluginAnswer{
				Question: question,
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
	}
}

func (*JobSurvey) convertUserInputPluginToString(val interface{}) (string, error) {
	var responseStr string
	switch reflect.TypeOf(val).Name() {
	case "int":
		responseStr = strconv.Itoa(val.(int))
	case "string":
		responseStr = val.(string)
	case "OptionAnswer":
		responseStr = val.(survey.OptionAnswer).Value
	default:
		return "", fmt.Errorf("unknown type found while parsing input: %v", val)
	}
	return responseStr, nil
}
