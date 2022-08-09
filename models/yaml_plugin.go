package models

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	"github.com/AlecAivazis/survey/v2"
)

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'

// USED in models.PluginQuestion Validations
type vFactory struct{}

func (*vFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		if !regex.Match([]byte(val)) {
			return fmt.Errorf("%s", message)
		}
		return nil
	}
}

var ValidatorFactory = new(vFactory)

type PluginSpec struct {
	PluginInfoResponse    `yaml:",inline"`
	GetQuestionsResponse  `yaml:",inline"` // PluginQuestion has extra attrs related to validation
	DefaultAssetsResponse `yaml:",inline"`
	DefaultConfigResponse `yaml:",inline"`
}

func (p *PluginSpec) PluginInfo() (*PluginInfoResponse, error) { // nolint
	return &PluginInfoResponse{
		Name:          p.Name,
		Description:   p.Description,
		Image:         p.Image,
		SecretPath:    p.SecretPath,
		PluginType:    p.PluginType,
		PluginMods:    p.PluginMods,
		PluginVersion: p.PluginVersion,
		HookType:      p.HookType,
		DependsOn:     p.DependsOn,
		APIVersion:    p.APIVersion,
	}, nil
}

func (p *PluginSpec) GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error) { //nolint
	return &GetQuestionsResponse{
		Questions: p.Questions,
	}, nil
}

func (p *PluginSpec) ValidateQuestion(_ context.Context, req ValidateQuestionRequest) (*ValidateQuestionResponse, error) { //nolint
	question := req.Answer.Question
	value := req.Answer.Value
	if err := question.isValid(value); err != nil {
		return &ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &ValidateQuestionResponse{
		Success: true,
	}, nil
}

func (p *PluginSpec) DefaultConfig(_ context.Context, req DefaultConfigRequest) (*DefaultConfigResponse, error) { //nolint
	conf := []PluginConfig{}

	// config from survey answers
	for _, ans := range req.Answers {
		conf = append(conf, PluginConfig{
			Name:  ans.Question.Name,
			Value: ans.Value,
		})
	}

	// adding defaultconfig (static, macros & referential config) from yaml
	conf = append(conf, p.Config...)

	return &DefaultConfigResponse{
		Config: conf,
	}, nil
}

func (p *PluginSpec) DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error) { //nolint
	return &DefaultAssetsResponse{
		Assets: p.Assets,
	}, nil
}

func (*PluginSpec) CompileAssets(_ context.Context, req CompileAssetsRequest) (*CompileAssetsResponse, error) { //nolint
	return &CompileAssetsResponse{
		Assets: req.Assets,
	}, nil
}
